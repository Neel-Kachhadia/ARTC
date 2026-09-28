#include "artc/rpc/services.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <limits>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include <grpcpp/alarm.h>
#include <grpc/support/time.h>

namespace artc::rpc {
namespace {

using namespace std::chrono_literals;
constexpr std::uint32_t kMaximumWorkUnits = 10'000;
constexpr std::uint32_t kMaximumPayloadBytes = 1'048'576;
constexpr std::uint64_t kMaximumDelayUs = 60'000'000;

gpr_timespec monotonic_deadline(std::chrono::microseconds delay) {
  return gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                      gpr_time_from_micros(delay.count(), GPR_TIMESPAN));
}

grpc::Status validate_request(const artc::v1::WorkRequest& request,
                              std::uint64_t delay_per_work_unit_us,
                              std::chrono::microseconds* delay) {
  if (request.work_units() > kMaximumWorkUnits ||
      request.payload_bytes() > kMaximumPayloadBytes) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "work request exceeds configured bounds"};
  }
  const std::uint64_t units = std::max<std::uint32_t>(1U, request.work_units());
  if (delay_per_work_unit_us > kMaximumDelayUs / units) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "requested work duration exceeds bound"};
  }
  *delay = std::chrono::microseconds(delay_per_work_unit_us * units);
  return grpc::Status::OK;
}

class CompletionState : public std::enable_shared_from_this<CompletionState> {
 public:
  explicit CompletionState(grpc::ServerUnaryReactor* reactor) : reactor_(reactor) {}
  virtual ~CompletionState() = default;
  virtual void cancel() {}

  void finish(grpc::Status status) {
    bool expected = false;
    if (finished_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      reactor_->Finish(std::move(status));
    }
  }

 private:
  grpc::ServerUnaryReactor* reactor_;
  std::atomic<bool> finished_{false};
};

class OwnedServerReactor final : public grpc::ServerUnaryReactor {
 public:
  void bind(std::shared_ptr<CompletionState> state) { state_ = std::move(state); }
  void OnCancel() override {
    if (state_) state_->cancel();
  }
  void OnDone() override { delete this; }

 private:
  std::shared_ptr<CompletionState> state_;
};

class DependencyRequest {
 public:
  grpc::ClientContext context;
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
};

class DelayState : public CompletionState {
 public:
  DelayState(grpc::ServerUnaryReactor* reactor, std::chrono::microseconds delay)
      : CompletionState(reactor), delay_(delay) {}

  void start() {
    if (delay_ == std::chrono::microseconds::zero()) {
      finish(grpc::Status::OK);
      return;
    }
    bool cancelled = false;
    {
      std::lock_guard lock(alarm_mutex_);
      cancelled = cancelled_;
      if (!cancelled) {
        std::weak_ptr<CompletionState> weak_self = shared_from_this();
        alarm_.Set(monotonic_deadline(delay_), [weak_self](bool ok) {
          if (const auto self = weak_self.lock()) {
            self->finish(ok ? grpc::Status::OK
                            : grpc::Status(grpc::StatusCode::CANCELLED,
                                           "server call cancelled"));
          }
        });
      }
    }
    if (cancelled) {
      finish({grpc::StatusCode::CANCELLED, "server call cancelled"});
    }
  }

  void cancel() override {
    std::lock_guard lock(alarm_mutex_);
    cancelled_ = true;
    alarm_.Cancel();
  }

 private:
  std::chrono::microseconds delay_;
  std::mutex alarm_mutex_;
  bool cancelled_{false};
  grpc::Alarm alarm_;
};

class ACallState final : public CompletionState {
 public:
  ACallState(grpc::ServerUnaryReactor* reactor, grpc::CallbackServerContext* context,
             const artc::v1::WorkRequest& request, artc::v1::WorkResponse* response,
             std::string replica_id, std::uint64_t delay_per_work_unit_us,
             std::shared_ptr<artc::v1::Traffic::Stub> dependency_stub)
      : CompletionState(reactor),
        context_(context),
        request_(request),
        response_(response),
        replica_id_(std::move(replica_id)),
        delay_per_work_unit_us_(delay_per_work_unit_us),
        dependency_stub_(std::move(dependency_stub)) {}

  void start() {
    std::chrono::microseconds delay;
    const grpc::Status status = validate_request(request_, delay_per_work_unit_us_, &delay);
    if (!status.ok()) {
      finish(status);
      return;
    }
    delay_ = delay;
    response_->set_request_id(request_.request_id());
    response_->set_replica_id(replica_id_);
    response_->set_backend_attempt_count(0);
    if (!request_.invoke_dependency()) {
      schedule_delay();
      return;
    }
    if (!dependency_stub_) {
      finish({grpc::StatusCode::FAILED_PRECONDITION,
              "dependency invocation requested without Service B"});
      return;
    }
    start_dependency();
  }

  void dependency_done(const grpc::Status& status,
                       const artc::v1::WorkResponse& dependency_response) {
    if (!status.ok()) {
      finish(status);
      return;
    }
    if (!cancelled_.load(std::memory_order_acquire)) {
      response_->set_dependency_id(dependency_response.replica_id());
    }
    schedule_delay();
  }

  void cancel() override {
    cancelled_.store(true, std::memory_order_release);
    std::shared_ptr<DependencyRequest> request;
    {
      std::lock_guard lock(dependency_mutex_);
      request = dependency_request_.lock();
    }
    if (request) request->context.TryCancel();
    {
      std::lock_guard lock(alarm_mutex_);
      alarm_.Cancel();
    }
  }

 private:
  void start_dependency();
  void schedule_delay() {
    if (delay_ == std::chrono::microseconds::zero()) {
      finish(cancelled_.load(std::memory_order_acquire)
                 ? grpc::Status(grpc::StatusCode::CANCELLED, "server call cancelled")
                 : grpc::Status::OK);
      return;
    }
    bool cancelled = false;
    {
      std::lock_guard lock(alarm_mutex_);
      cancelled = cancelled_.load(std::memory_order_acquire);
      if (!cancelled) {
        std::weak_ptr<ACallState> weak_self =
            std::static_pointer_cast<ACallState>(shared_from_this());
        alarm_.Set(monotonic_deadline(delay_), [weak_self](bool ok) {
          auto state = weak_self.lock();
          if (!state) return;
          if (state->cancelled_.load(std::memory_order_acquire)) {
            state->finish({grpc::StatusCode::CANCELLED, "server call cancelled"});
            return;
          }
          state->finish(ok ? grpc::Status::OK
                           : grpc::Status(grpc::StatusCode::CANCELLED,
                                          "server call cancelled"));
        });
      }
    }
    if (cancelled) {
      finish({grpc::StatusCode::CANCELLED, "server call cancelled"});
    }
  }

  grpc::CallbackServerContext* context_;
  const artc::v1::WorkRequest request_;
  artc::v1::WorkResponse* response_;
  std::string replica_id_;
  std::uint64_t delay_per_work_unit_us_;
  std::shared_ptr<artc::v1::Traffic::Stub> dependency_stub_;
  std::chrono::microseconds delay_{0};
  std::mutex dependency_mutex_;
  std::weak_ptr<DependencyRequest> dependency_request_;
  std::mutex alarm_mutex_;
  grpc::Alarm alarm_;
  std::atomic<bool> cancelled_{false};
};

class DependencyReactor final : public grpc::ClientUnaryReactor {
 public:
  DependencyReactor(std::shared_ptr<ACallState> parent,
                    std::shared_ptr<DependencyRequest> request)
      : parent_(std::move(parent)), request_(std::move(request)) {}

  void OnDone(const grpc::Status& status) override {
    parent_->dependency_done(status, request_->response);
    delete this;
  }

 private:
  std::shared_ptr<ACallState> parent_;
  std::shared_ptr<DependencyRequest> request_;
};

void ACallState::start_dependency() {
  auto request = std::make_shared<DependencyRequest>();
  request->request.set_request_id(request_.request_id());
  request->request.set_work_units(request_.work_units());
  request->request.set_payload_bytes(request_.payload_bytes());
  request->context.set_deadline(context_->deadline());
  bool cancelled = false;
  {
    std::lock_guard lock(dependency_mutex_);
    cancelled = cancelled_.load(std::memory_order_acquire);
    if (!cancelled) {
      dependency_request_ = request;
      auto reactor = std::make_unique<DependencyReactor>(
          std::static_pointer_cast<ACallState>(shared_from_this()), request);
      dependency_stub_->async()->Execute(&request->context, &request->request,
                                         &request->response, reactor.get());
      reactor->StartCall();
      static_cast<void>(reactor.release());
    }
  }
  if (cancelled) {
    finish({grpc::StatusCode::CANCELLED, "server call cancelled"});
  }
}

}  // namespace

struct RouterService::State : public std::enable_shared_from_this<RouterService::State> {
  struct Backend {
    std::shared_ptr<routing::ReplicaState> state;
    std::shared_ptr<artc::v1::Traffic::Stub> stub;
  };

  State(std::vector<ReplicaConfig> configs, routing::Policy policy,
        std::uint64_t seed, double smoothing)
      : selector(routing::make_selector(policy, seed, smoothing)) {
    if (configs.empty()) throw std::invalid_argument("router requires at least one replica");
    for (auto& config : configs) {
      if (config.id.empty() || config.address.empty()) {
        throw std::invalid_argument("replica id and address must be non-empty");
      }
      if (std::any_of(backends.begin(), backends.end(), [&](const Backend& backend) {
            return backend.state->id == config.id || backend.state->address == config.address;
          })) {
        throw std::invalid_argument("replica ids and addresses must be unique");
      }
      auto channel = grpc::CreateChannel(config.address, grpc::InsecureChannelCredentials());
      auto stub = std::shared_ptr<artc::v1::Traffic::Stub>(
          artc::v1::Traffic::NewStub(channel).release());
      states.push_back(std::make_shared<routing::ReplicaState>(config.id, config.address));
      backends.push_back({states.back(), std::move(stub)});
    }
  }

  routing::ReplicaLease reserve(std::size_t* backend_index) {
    auto lease = selector->select(states);
    const auto target = lease.replica();
    const auto found = std::find_if(backends.begin(), backends.end(), [&](const Backend& backend) {
      return backend.state == target;
    });
    if (found == backends.end()) throw std::logic_error("selector returned unknown replica");
    // ponytail: linear lookup over fixed lab pool; use an index if pool grows beyond three replicas.
    *backend_index = static_cast<std::size_t>(std::distance(backends.begin(), found));
    return lease;
  }

  std::unique_ptr<routing::Selector> selector;
  std::vector<std::shared_ptr<routing::ReplicaState>> states;
  std::vector<Backend> backends;
};

namespace {

class RouterCallState final : public CompletionState {
 public:
  RouterCallState(grpc::ServerUnaryReactor* reactor, grpc::CallbackServerContext* context,
                  artc::v1::WorkResponse* response, std::shared_ptr<RouterService::State> owner,
                  routing::ReplicaLease lease, std::size_t backend_index)
      : CompletionState(reactor),
        context_(context),
        response_(response),
        owner_(std::move(owner)),
        lease_(std::move(lease)),
        backend_index_(backend_index),
        started_(std::chrono::steady_clock::now()) {}

  void start(const artc::v1::WorkRequest& request);

  void cancel() override {
    cancelled_.store(true, std::memory_order_release);
    std::lock_guard lock(downstream_mutex_);
    if (downstream_started_) downstream_->context.TryCancel();
  }

  void backend_done(const grpc::Status& status,
                    const artc::v1::WorkResponse& backend_response) {
    const auto now = std::chrono::steady_clock::now();
    const double latency_us = std::chrono::duration<double, std::micro>(now - started_).count();
    const bool cancelled = cancelled_.load(std::memory_order_acquire);
    grpc::Status completion_status = status;
    if (status.ok() && !cancelled) {
      owner_->selector->record_latency(lease_.replica(), latency_us);
      response_->CopyFrom(backend_response);
      response_->set_backend_attempt_count(1);
    } else {
      context_->AddTrailingMetadata("artc-selected-replica", lease_.replica()->id);
      context_->AddTrailingMetadata("artc-backend-attempts", "1");
      if (cancelled && status.ok()) {
        completion_status = {grpc::StatusCode::CANCELLED, "router request cancelled"};
      }
    }
    lease_.release();
    finish(std::move(completion_status));
  }

 private:
  class BackendRequest final : public DependencyRequest {};
  class BackendReactor;

  grpc::CallbackServerContext* context_;
  artc::v1::WorkResponse* response_;
  std::shared_ptr<RouterService::State> owner_;
  routing::ReplicaLease lease_;
  std::size_t backend_index_;
  std::chrono::steady_clock::time_point started_;
  std::shared_ptr<BackendRequest> downstream_{std::make_shared<BackendRequest>()};
  std::mutex downstream_mutex_;
  bool downstream_started_{false};
  std::atomic<bool> cancelled_{false};
};

class RouterCallState::BackendReactor final : public grpc::ClientUnaryReactor {
 public:
  BackendReactor(std::shared_ptr<RouterCallState> parent,
                 std::shared_ptr<BackendRequest> request)
      : parent_(std::move(parent)), request_(std::move(request)) {}

  void OnDone(const grpc::Status& status) override {
    parent_->backend_done(status, request_->response);
    delete this;
  }

 private:
  std::shared_ptr<RouterCallState> parent_;
  std::shared_ptr<BackendRequest> request_;
};

void RouterCallState::start(const artc::v1::WorkRequest& request) {
  bool cancelled = false;
  {
    std::lock_guard lock(downstream_mutex_);
    cancelled = cancelled_.load(std::memory_order_acquire);
    if (!cancelled) {
      downstream_->context.set_deadline(context_->deadline());
      downstream_->request = request;
      auto reactor = std::make_unique<BackendReactor>(
          std::static_pointer_cast<RouterCallState>(shared_from_this()), downstream_);
      auto& stub = owner_->backends[backend_index_].stub;
      stub->async()->Execute(&downstream_->context, &downstream_->request,
                             &downstream_->response, reactor.get());
      downstream_started_ = true;
      reactor->StartCall();
      static_cast<void>(reactor.release());
    }
  }
  if (cancelled) {
    lease_.release();
    finish({grpc::StatusCode::CANCELLED, "router request cancelled"});
  }
}

}  // namespace

ServiceA::ServiceA(std::string replica_id, std::uint64_t delay_per_work_unit_us,
                   std::string dependency_address)
    : replica_id_(std::move(replica_id)),
      delay_per_work_unit_us_(delay_per_work_unit_us) {
  if (replica_id_.empty() || delay_per_work_unit_us_ > kMaximumDelayUs) {
    throw std::invalid_argument("invalid Service-A configuration");
  }
  if (!dependency_address.empty()) {
    auto channel = grpc::CreateChannel(std::move(dependency_address),
                                       grpc::InsecureChannelCredentials());
    dependency_stub_.reset(artc::v1::Traffic::NewStub(channel).release());
  }
}

grpc::ServerUnaryReactor* ServiceA::Execute(
    grpc::CallbackServerContext* context, const artc::v1::WorkRequest* request,
    artc::v1::WorkResponse* response) {
  auto* reactor = new OwnedServerReactor();
  auto state = std::make_shared<ACallState>(reactor, context, *request, response,
                                            replica_id_, delay_per_work_unit_us_,
                                            dependency_stub_);
  reactor->bind(state);
  state->start();
  return reactor;
}

grpc::ServerUnaryReactor* ServiceA::Health(
    grpc::CallbackServerContext* context, const artc::v1::HealthRequest*,
    artc::v1::HealthResponse* response) {
  auto* reactor = context->DefaultReactor();
  response->set_ready(true);
  response->set_component_id(replica_id_);
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

ServiceB::ServiceB(std::string component_id, std::uint64_t delay_per_work_unit_us)
    : component_id_(std::move(component_id)), delay_per_work_unit_us_(delay_per_work_unit_us) {
  if (component_id_.empty() || delay_per_work_unit_us_ > kMaximumDelayUs) {
    throw std::invalid_argument("invalid Service-B configuration");
  }
}

grpc::ServerUnaryReactor* ServiceB::Execute(
    grpc::CallbackServerContext*, const artc::v1::WorkRequest* request,
    artc::v1::WorkResponse* response) {
  auto* reactor = new OwnedServerReactor();
  std::chrono::microseconds delay{0};
  const grpc::Status status = validate_request(*request, delay_per_work_unit_us_, &delay);
  response->set_request_id(request->request_id());
  response->set_replica_id(component_id_);
  response->set_backend_attempt_count(0);
  auto state = std::make_shared<DelayState>(reactor, delay);
  reactor->bind(state);
  if (!status.ok()) state->finish(status);
  else state->start();
  return reactor;
}

grpc::ServerUnaryReactor* ServiceB::Health(
    grpc::CallbackServerContext* context, const artc::v1::HealthRequest*,
    artc::v1::HealthResponse* response) {
  auto* reactor = context->DefaultReactor();
  response->set_ready(true);
  response->set_component_id(component_id_);
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

RouterService::RouterService(std::vector<ReplicaConfig> replicas, routing::Policy policy,
                             std::uint64_t seed, double ewma_smoothing)
    : state_(std::make_shared<State>(std::move(replicas), policy, seed, ewma_smoothing)) {}

grpc::ServerUnaryReactor* RouterService::Execute(
    grpc::CallbackServerContext* context, const artc::v1::WorkRequest* request,
    artc::v1::WorkResponse* response) {
  auto* reactor = new OwnedServerReactor();
  std::chrono::microseconds ignored_delay{0};
  const grpc::Status validation = validate_request(*request, 0, &ignored_delay);
  if (!validation.ok()) {
    reactor->Finish(validation);
    return reactor;
  }
  std::size_t backend_index = 0;
  try {
    auto lease = state_->reserve(&backend_index);
    auto state = std::make_shared<RouterCallState>(reactor, context, response, state_,
                                                   std::move(lease), backend_index);
    reactor->bind(state);
    state->start(*request);
  } catch (const std::exception& error) {
    reactor->Finish({grpc::StatusCode::INTERNAL, error.what()});
  }
  return reactor;
}

grpc::ServerUnaryReactor* RouterService::Health(
    grpc::CallbackServerContext* context, const artc::v1::HealthRequest*,
    artc::v1::HealthResponse* response) {
  auto* reactor = context->DefaultReactor();
  response->set_ready(true);
  response->set_component_id("artc-router");
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

std::unique_ptr<grpc::Server> start_server(std::string_view address,
                                           grpc::Service& service,
                                           int* selected_port) {
  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(static_cast<int>(kMaximumPayloadBytes));
  builder.SetMaxSendMessageSize(static_cast<int>(kMaximumPayloadBytes));
  int bound_port = 0;
  builder.AddListeningPort(std::string(address), grpc::InsecureServerCredentials(),
                           selected_port == nullptr ? nullptr : &bound_port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server) throw std::runtime_error("gRPC server failed to start");
  if (selected_port != nullptr) *selected_port = bound_port;
  return server;
}

void block_shutdown_signals() {
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
    throw std::runtime_error("failed to block shutdown signals");
  }
}

int wait_for_shutdown(grpc::Server& server) {
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  std::jthread shutdown_thread([&server, signals](std::stop_token stop) {
    while (!stop.stop_requested()) {
      timespec timeout{0, 100'000'000};
      const int signal_number = sigtimedwait(&signals, nullptr, &timeout);
      if (signal_number == SIGINT || signal_number == SIGTERM) {
        server.Shutdown(std::chrono::system_clock::now() + 5s);
        return;
      }
    }
  });
  server.Wait();
  shutdown_thread.request_stop();
  shutdown_thread.join();
  return 0;
}

bool check_health(std::string_view target, std::string* component_id) {
  auto channel = grpc::CreateChannel(std::string(target), grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 1s);
  artc::v1::HealthRequest request;
  artc::v1::HealthResponse response;
  const grpc::Status status = stub->Health(&context, request, &response);
  if (!status.ok() || !response.ready()) return false;
  if (component_id != nullptr) *component_id = response.component_id();
  return true;
}

}  // namespace artc::rpc
