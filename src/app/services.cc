#include "artc/rpc/services.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include <grpcpp/alarm.h>
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpc/support/time.h>

namespace artc::rpc {
namespace {

using namespace std::chrono_literals;
constexpr std::uint32_t kMaximumWorkUnits = 10'000;
constexpr std::uint32_t kMaximumPayloadBytes = 1'048'576;
constexpr std::uint64_t kMaximumDelayUs = 60'000'000;

class NoHealthyReplica final : public std::runtime_error {
 public:
  NoHealthyReplica() : std::runtime_error("no healthy replica") {}
};

const char* health_name(routing::HealthState health) noexcept {
  switch (health) {
    case routing::HealthState::kHealthy: return "healthy";
    case routing::HealthState::kDegraded: return "degraded";
    case routing::HealthState::kRecovering: return "recovering";
    case routing::HealthState::kUnavailable: return "unavailable";
  }
  return "unknown";
}

gpr_timespec monotonic_deadline(std::chrono::microseconds delay) {
  return gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                      gpr_time_from_micros(delay.count(), GPR_TIMESPAN));
}

std::shared_ptr<grpc::Channel> create_backend_channel(std::string address) {
  grpc::ChannelArguments arguments;
  arguments.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
  arguments.SetInt(GRPC_ARG_EXPERIMENTAL_ENABLE_HEDGING, 0);
  return grpc::CreateCustomChannel(std::move(address),
                                   grpc::InsecureChannelCredentials(), arguments);
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

  State(std::vector<ReplicaConfig> configs, routing::Policy selected_policy,
        std::uint64_t seed, double smoothing,
        control::ControllerConfig controller_config)
      : selector(routing::make_selector(selected_policy, seed, smoothing)),
        policy(selected_policy) {
    if (configs.empty()) throw std::invalid_argument("router requires at least one replica");
    for (auto& replica_config : configs) {
      if (replica_config.id.empty() || replica_config.address.empty()) {
        throw std::invalid_argument("replica id and address must be non-empty");
      }
      if (std::any_of(backends.begin(), backends.end(), [&](const Backend& backend) {
            return backend.state->id == replica_config.id ||
                   backend.state->address == replica_config.address;
          })) {
        throw std::invalid_argument("replica ids and addresses must be unique");
      }
      auto channel = create_backend_channel(replica_config.address);
      auto stub = std::shared_ptr<artc::v1::Traffic::Stub>(
          artc::v1::Traffic::NewStub(channel).release());
      states.push_back(std::make_shared<routing::ReplicaState>(replica_config.id,
                                                               replica_config.address));
      backends.push_back({states.back(), std::move(stub)});
    }

    switch (policy) {
      case routing::Policy::kArtcSelectorOnly:
        controller_config.adaptive_selector = true;
        controller_config.adaptive_concurrency = false;
        controller_config.deadline_feasibility = false;
        break;
      case routing::Policy::kAdaptiveConcurrencyOnly:
        controller_config.adaptive_selector = false;
        controller_config.adaptive_concurrency = true;
        controller_config.deadline_feasibility = false;
        break;
      case routing::Policy::kArtcAdaptiveNoDeadline:
        controller_config.adaptive_selector = true;
        controller_config.adaptive_concurrency = true;
        controller_config.deadline_feasibility = false;
        break;
      case routing::Policy::kArtcAdaptive:
        controller_config.adaptive_selector = true;
        controller_config.adaptive_concurrency = true;
        controller_config.deadline_feasibility = true;
        break;
      case routing::Policy::kRoundRobin:
      case routing::Policy::kLeastInflight:
      case routing::Policy::kEwmaLatency:
      case routing::Policy::kP2CLatencyInflight:
        return;
    }
    controller = std::make_unique<control::Phase2Controller>(controller_config, states);
    if (controller_config.adaptive_selector) {
      adaptive_selector = std::make_unique<control::AdaptiveSelector>(controller_config);
    }
    controller->start();
  }

  [[nodiscard]] bool phase2_enabled() const noexcept { return controller != nullptr; }

  [[nodiscard]] bool should_sample_decision() noexcept {
    const auto every = controller ? controller->config().decision_sample_every : 0;
    return every != 0 && decision_sequence.fetch_add(1, std::memory_order_relaxed) % every == 0;
  }

  routing::ReplicaLease reserve(std::size_t* backend_index,
                                std::uint64_t request_id,
                                std::shared_ptr<const control::ControllerSnapshot>* snapshot,
                                control::CandidateScore* selected_score) {
    if (controller) *snapshot = controller->snapshot();
    if (adaptive_selector) {
      const auto selection = adaptive_selector->select(**snapshot, states, request_id);
      if (!selection.selected) throw NoHealthyReplica();
      *backend_index = *selection.selected;
      *selected_score = selection.selected_score;
      return routing::ReplicaLease(states[*backend_index]);
    }
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
  routing::Policy policy;
  std::vector<std::shared_ptr<routing::ReplicaState>> states;
  std::vector<Backend> backends;
  std::unique_ptr<control::AdaptiveSelector> adaptive_selector;
  std::unique_ptr<control::Phase2Controller> controller;
  std::atomic<std::uint64_t> decision_sequence{0};
};

namespace {

class RouterCallState final : public CompletionState {
 public:
  RouterCallState(grpc::ServerUnaryReactor* reactor, grpc::CallbackServerContext* context,
                  artc::v1::WorkResponse* response, std::shared_ptr<RouterService::State> owner,
                  control::RequestContext request_context,
                  routing::ReplicaLease lease, control::AdmissionPermit permit,
                  std::size_t backend_index, std::string decision_metadata)
      : CompletionState(reactor),
        context_(context),
        response_(response),
        owner_(std::move(owner)),
        request_context_(std::move(request_context)),
        lease_(std::move(lease)),
        permit_(std::move(permit)),
        backend_index_(backend_index),
        decision_metadata_(std::move(decision_metadata)),
        started_(request_context_.arrival_time) {}

  void start(const artc::v1::WorkRequest& request);

  void cancel() override {
    auto expected = TerminalState::kOpen;
    if (!terminal_.compare_exchange_strong(expected, TerminalState::kCancelled,
                                           std::memory_order_acq_rel)) {
      return;
    }
    std::lock_guard lock(downstream_mutex_);
    if (downstream_started_) downstream_->context.TryCancel();
  }

  void backend_done(const grpc::Status& status,
                    const artc::v1::WorkResponse& backend_response) {
    auto terminal = terminal_.load(std::memory_order_acquire);
    for (;;) {
      if (terminal == TerminalState::kCompleting) return;
      if (terminal_.compare_exchange_weak(terminal, TerminalState::kCompleting,
                                          std::memory_order_acq_rel)) {
        break;
      }
    }
    const auto now = control::SteadyClock::now();
    const double latency_us = std::chrono::duration<double, std::micro>(now - started_).count();
    const bool cancelled = terminal == TerminalState::kCancelled;
    grpc::Status completion_status = status;
    control::RequestOutcome outcome = control::RequestOutcome::kFailure;
    if (cancelled) {
      outcome = control::RequestOutcome::kCancelled;
      if (status.ok()) {
        completion_status = {grpc::StatusCode::CANCELLED, "router request cancelled"};
      }
    } else if (owner_->controller &&
               (now >= request_context_.effective_deadline ||
                status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)) {
      outcome = status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED
                    ? control::RequestOutcome::kTimeout
                    : control::RequestOutcome::kDeadlineMiss;
      completion_status = {grpc::StatusCode::DEADLINE_EXCEEDED,
                           "request deadline exhausted in ARTC"};
    } else if (status.ok()) {
      outcome = control::RequestOutcome::kSuccess;
      response_->CopyFrom(backend_response);
      response_->set_backend_attempt_count(1);
    } else {
      outcome = status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED
                    ? control::RequestOutcome::kTimeout
                    : control::RequestOutcome::kFailure;
    }
    if (owner_->controller) {
      static_cast<void>(owner_->controller->record_completion(
          backend_index_, now, latency_us, outcome));
    } else if (status.ok() && !cancelled) {
      owner_->selector->record_latency(lease_.replica(), latency_us);
    }
    request_context_.terminal_outcome = outcome;
    context_->AddTrailingMetadata("artc-admission-result",
                                  control::admission_result_name(
                                      request_context_.admission_result));
    context_->AddTrailingMetadata("artc-backend-attempts", "1");
    context_->AddTrailingMetadata("artc-selected-replica", lease_.replica()->id);
    if (!decision_metadata_.empty()) {
      context_->AddTrailingMetadata("artc-decision", decision_metadata_);
    }
    lease_.release();
    permit_.release();
    finish(std::move(completion_status));
  }

 private:
  enum class TerminalState : std::uint8_t { kOpen, kCancelled, kCompleting };
  class BackendRequest final : public DependencyRequest {};
  class BackendReactor;

  grpc::CallbackServerContext* context_;
  artc::v1::WorkResponse* response_;
  std::shared_ptr<RouterService::State> owner_;
  control::RequestContext request_context_;
  routing::ReplicaLease lease_;
  control::AdmissionPermit permit_;
  std::size_t backend_index_;
  std::string decision_metadata_;
  control::SteadyTime started_;
  std::shared_ptr<BackendRequest> downstream_{std::make_shared<BackendRequest>()};
  std::mutex downstream_mutex_;
  bool downstream_started_{false};
  std::atomic<TerminalState> terminal_{TerminalState::kOpen};
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
  bool expired = false;
  {
    std::lock_guard lock(downstream_mutex_);
    cancelled = terminal_.load(std::memory_order_acquire) == TerminalState::kCancelled ||
                context_->IsCancelled();
    if (cancelled) {
      auto expected = TerminalState::kOpen;
      static_cast<void>(terminal_.compare_exchange_strong(
          expected, TerminalState::kCancelled, std::memory_order_acq_rel));
    }
    if (!cancelled) {
      if (owner_->controller) {
        const auto steady_now = control::SteadyClock::now();
        expired = request_context_.remaining_budget(steady_now) ==
                  std::chrono::nanoseconds::zero();
        if (!expired) {
          downstream_->context.set_deadline(request_context_.downstream_deadline(
              steady_now, std::chrono::system_clock::now()));
        }
      } else {
        downstream_->context.set_deadline(context_->deadline());
      }
    }
    if (!cancelled && !expired) {
      downstream_->request = request;
      auto reactor = std::make_unique<BackendReactor>(
          std::static_pointer_cast<RouterCallState>(shared_from_this()), downstream_);
      auto& stub = owner_->backends[backend_index_].stub;
      stub->async()->Execute(&downstream_->context, &downstream_->request,
                             &downstream_->response, reactor.get());
      downstream_started_ = true;
      if (owner_->controller) owner_->controller->record_backend_attempt(backend_index_);
      reactor->StartCall();
      static_cast<void>(reactor.release());
    }
  }
  if (cancelled) {
    terminal_.store(TerminalState::kCompleting, std::memory_order_release);
    request_context_.terminal_outcome = control::RequestOutcome::kCancelled;
    lease_.release();
    permit_.release();
    context_->AddTrailingMetadata("artc-backend-attempts", "0");
    context_->AddTrailingMetadata("artc-admission-result",
                                  control::admission_result_name(
                                      request_context_.admission_result));
    context_->AddTrailingMetadata("artc-selected-replica", lease_.replica()->id);
    if (!decision_metadata_.empty()) context_->AddTrailingMetadata("artc-decision", decision_metadata_);
    finish({grpc::StatusCode::CANCELLED, "router request cancelled"});
  } else if (expired) {
    auto expected = TerminalState::kOpen;
    if (!terminal_.compare_exchange_strong(expected, TerminalState::kCompleting,
                                           std::memory_order_acq_rel)) {
      if (expected == TerminalState::kCancelled) {
        request_context_.terminal_outcome = control::RequestOutcome::kCancelled;
        lease_.release();
        permit_.release();
        context_->AddTrailingMetadata("artc-backend-attempts", "0");
        context_->AddTrailingMetadata("artc-admission-result",
                                      control::admission_result_name(
                                          request_context_.admission_result));
        finish({grpc::StatusCode::CANCELLED, "router request cancelled"});
        return;
      }
      return;
    }
    request_context_.terminal_outcome = control::RequestOutcome::kDeadlineMiss;
    lease_.release();
    permit_.release();
    if (owner_->controller) {
      owner_->controller->record_pre_dispatch_deadline_miss();
    }
    context_->AddTrailingMetadata("artc-backend-attempts", "0");
    context_->AddTrailingMetadata("artc-admission-result",
                                  control::admission_result_name(
                                      request_context_.admission_result));
    context_->AddTrailingMetadata("artc-selected-replica", lease_.replica()->id);
    if (!decision_metadata_.empty()) context_->AddTrailingMetadata("artc-decision", decision_metadata_);
    finish({grpc::StatusCode::DEADLINE_EXCEEDED,
            "request deadline expired before backend dispatch"});
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
    auto channel = create_backend_channel(std::move(dependency_address));
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
                             std::uint64_t seed, double ewma_smoothing,
                             control::ControllerConfig controller_config)
    : state_(std::make_shared<State>(std::move(replicas), policy, seed,
                                     ewma_smoothing, std::move(controller_config))) {}

RouterService::~RouterService() {
  if (state_ && state_->controller) state_->controller->close_admission();
}

grpc::ServerUnaryReactor* RouterService::Execute(
    grpc::CallbackServerContext* context, const artc::v1::WorkRequest* request,
    artc::v1::WorkResponse* response) {
  const auto request_arrival = control::SteadyClock::now();
  const auto request_arrival_system = std::chrono::system_clock::now();
  auto* reactor = new OwnedServerReactor();
  const auto controller = state_->controller.get();
  const auto reject = [&](control::AdmissionResult result, grpc::StatusCode status,
                          const char* message) {
    if (controller) controller->record_admission(result);
    context->AddTrailingMetadata("artc-admission-result",
                                 control::admission_result_name(result));
    context->AddTrailingMetadata("artc-backend-attempts", "0");
    reactor->Finish({status, message});
    return reactor;
  };
  std::chrono::microseconds ignored_delay{0};
  const grpc::Status validation = validate_request(*request, 0, &ignored_delay);
  if (!validation.ok()) {
    if (controller) {
      return reject(control::AdmissionResult::kInvalidRequest,
                    validation.error_code(), validation.error_message().c_str());
    }
    reactor->Finish(validation);
    return reactor;
  }

  if (context->IsCancelled()) {
    if (controller) {
      return reject(control::AdmissionResult::kCancelled,
                    grpc::StatusCode::CANCELLED, "request cancelled before admission");
    }
    reactor->Finish({grpc::StatusCode::CANCELLED, "request cancelled before routing"});
    return reactor;
  }

  control::RequestContext request_context;
  control::AdmissionPermit permit;
  std::shared_ptr<const control::ControllerSnapshot> snapshot;
  if (controller) {
    request_context = control::make_request_context(
        request->request_id(), context->deadline(), request_arrival,
        request_arrival_system,
        controller->config().default_deadline);
    snapshot = controller->snapshot();
    request_context.controller_version = snapshot->version;
    const auto admission_now = control::SteadyClock::now();
    if (request_context.remaining_budget(admission_now) ==
        std::chrono::nanoseconds::zero()) {
      return reject(control::AdmissionResult::kDeadlineExpired,
                    grpc::StatusCode::DEADLINE_EXCEEDED, "caller deadline already expired");
    }
    if (controller->config().deadline_feasibility) {
      const auto feasibility = control::evaluate_deadline_feasibility(
          request_context, admission_now, *snapshot,
          controller->config().deadline_safety_margin,
          std::chrono::microseconds(static_cast<std::int64_t>(
              controller->config().cold_start_latency_us)));
      if (!feasibility.feasible) {
        controller->record_deadline_infeasible();
        return reject(control::AdmissionResult::kDeadlineInfeasible,
                      grpc::StatusCode::DEADLINE_EXCEEDED,
                      "estimated completion exceeds caller deadline");
      }
    }

    auto acquired = controller->try_acquire();
    if (acquired.result != control::AdmissionResult::kAdmitted) {
      const auto code = acquired.result == control::AdmissionResult::kConcurrencyLimit
                            ? grpc::StatusCode::RESOURCE_EXHAUSTED
                            : grpc::StatusCode::UNAVAILABLE;
      return reject(acquired.result, code, "route admission rejected request");
    }
    permit = std::move(acquired.permit);
  } else {
    request_context.request_id = request->request_id();
    request_context.arrival_time = request_arrival;
    request_context.effective_deadline = control::SteadyTime::max();
    request_context.caller_deadline = context->deadline();
  }

  std::size_t backend_index = 0;
  control::CandidateScore selected_score;
  try {
    auto lease = state_->reserve(&backend_index, request->request_id(), &snapshot,
                                 &selected_score);
    if (controller && request_context.remaining_budget(control::SteadyClock::now()) ==
                          std::chrono::nanoseconds::zero()) {
      lease.release();
      permit.release();
      return reject(control::AdmissionResult::kDeadlineExpired,
                    grpc::StatusCode::DEADLINE_EXCEEDED,
                    "caller deadline expired before dispatch");
    }
    if (controller) {
      if (snapshot) request_context.controller_version = snapshot->version;
      controller->record_admission(control::AdmissionResult::kAdmitted);
    }
    request_context.admission_result = control::AdmissionResult::kAdmitted;
    request_context.selected_replica = backend_index;
    std::string decision_metadata;
    if (controller && state_->should_sample_decision()) {
      std::ostringstream sample;
      sample << std::setprecision(5) << "v=" << snapshot->version
             << ",limit=" << snapshot->route_limit
             << ",inflight=" << snapshot->route_inflight
             << ",selected=" << backend_index
             << ",score=" << selected_score.total
             << ",lat=" << selected_score.latency_component
             << ",load=" << selected_score.load_component
             << ",error=" << selected_score.error_component
             << ",health=" << selected_score.health_component
             << ",attempts=" << snapshot->backend_attempts
             << ",goodput=" << snapshot->deadline_goodput
             << ",deadline_misses=" << snapshot->deadline_misses
             << ",deadline_infeasible=" << snapshot->deadline_infeasible
             << ",limit_changes=" << snapshot->controller_limit_changes
             << ",overloads=" << snapshot->controller_overload_events
             << ",observation_drops=" << snapshot->observation_drops
             << ",admitted=" << snapshot->admission_counts[
                    static_cast<std::size_t>(control::AdmissionResult::kAdmitted)]
             << ",concurrency_rejected=" << snapshot->admission_counts[
                    static_cast<std::size_t>(control::AdmissionResult::kConcurrencyLimit)]
             << ",candidates=";
      std::vector<control::CandidateScore> scores;
      if (state_->adaptive_selector) scores = state_->adaptive_selector->explain(*snapshot, state_->states);
      for (std::size_t i = 0; i < snapshot->replicas.size(); ++i) {
        if (i != 0) sample << ';';
        const auto& replica = snapshot->replicas[i];
        sample << i << ':' << health_name(replica.health) << ':' << replica.inflight
               << ':' << replica.latency_ewma_us << ':' << replica.error_ewma;
        if (i < scores.size()) sample << ':' << scores[i].total;
      }
      decision_metadata = sample.str();
    }
    auto state = std::make_shared<RouterCallState>(reactor, context, response, state_,
                                                   std::move(request_context),
                                                   std::move(lease), std::move(permit),
                                                   backend_index,
                                                   std::move(decision_metadata));
    reactor->bind(state);
    state->start(*request);
  } catch (const NoHealthyReplica&) {
    permit.release();
    return reject(control::AdmissionResult::kNoHealthyReplica,
                  grpc::StatusCode::UNAVAILABLE, "no healthy replica available");
  } catch (const std::exception& error) {
    permit.release();
    if (controller) {
      context->AddTrailingMetadata("artc-admission-result", "REJECT_INTERNAL");
      context->AddTrailingMetadata("artc-backend-attempts", "0");
    }
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
