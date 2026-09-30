#include "artc/rpc/services.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <pthread.h>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <stop_token>
#include <system_error>
#include <thread>
#include <type_traits>
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

class BackendCallbackDrain {
 public:
  void register_callback() {
    std::lock_guard lock(mutex_);
    ++pending_;
  }

  void callback_destroyed() noexcept {
    std::lock_guard lock(mutex_);
    if (pending_ == 0) std::terminate();
    --pending_;
    if (pending_ == 0) cv_.notify_all();
  }

  [[nodiscard]] std::uint64_t pending() const noexcept {
    std::lock_guard lock(mutex_);
    return pending_;
  }

  [[nodiscard]] bool wait_until(std::chrono::steady_clock::time_point deadline) const {
    std::unique_lock lock(mutex_);
    return cv_.wait_until(lock, deadline, [&] { return pending_ == 0; });
  }

 private:
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  std::uint64_t pending_{0};
};

constexpr std::size_t cancellation_reason_index(
    AttemptCancellationReason reason) noexcept {
  return static_cast<std::size_t>(reason);
}

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

  bool finish(grpc::Status status) {
    bool expected = false;
    if (finished_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      reactor_->Finish(std::move(status));
      return true;
    }
    return false;
  }

  [[nodiscard]] bool finished() const noexcept {
    return finished_.load(std::memory_order_acquire);
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
             std::shared_ptr<artc::v1::Traffic::Stub> dependency_stub,
             grpc::Status injected_status, bool honor_cancellation,
             std::shared_ptr<ServiceAWorkMetrics> work_metrics)
      : CompletionState(reactor),
        context_(context),
        request_(request),
        response_(response),
        replica_id_(std::move(replica_id)),
        delay_per_work_unit_us_(delay_per_work_unit_us),
        dependency_stub_(std::move(dependency_stub)),
        injected_status_(std::move(injected_status)),
        honor_cancellation_(honor_cancellation),
        work_metrics_(std::move(work_metrics)) {}

  void start() {
    std::chrono::microseconds delay;
    const grpc::Status status = validate_request(request_, delay_per_work_unit_us_, &delay);
    if (!status.ok()) {
      complete(status);
      return;
    }
    delay_ = delay;
    response_->set_request_id(request_.request_id());
    response_->set_replica_id(replica_id_);
    response_->set_backend_attempt_count(0);
    if (!injected_status_.ok()) {
      complete(injected_status_);
      return;
    }
    if (!request_.invoke_dependency()) {
      schedule_delay();
      return;
    }
    if (!dependency_stub_) {
      complete({grpc::StatusCode::FAILED_PRECONDITION,
              "dependency invocation requested without Service B"});
      return;
    }
    start_dependency();
  }

  void dependency_done(const grpc::Status& status,
                       const artc::v1::WorkResponse& dependency_response) {
    if (!status.ok()) {
      complete(status);
      return;
    }
    if (!honor_cancellation_ || !cancelled_.load(std::memory_order_acquire)) {
      response_->set_dependency_id(dependency_response.replica_id());
    }
    schedule_delay();
  }

  void cancel() override {
    if (finished()) return;
    {
      std::lock_guard lock(work_mutex_);
      if (!finished() && !cancel_time_) {
        cancel_time_ = std::chrono::steady_clock::now();
        work_metrics_->cancellation_signals.fetch_add(1, std::memory_order_relaxed);
      }
    }
    cancelled_.store(true, std::memory_order_release);
    if (!honor_cancellation_) return;
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
  void complete(grpc::Status status) {
    if (!finish(std::move(status))) return;
    const auto finished_at = std::chrono::steady_clock::now();
    work_metrics_->completed.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard lock(work_mutex_);
    if (cancel_time_ && finished_at > *cancel_time_) {
      const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
          finished_at - *cancel_time_).count();
      if (elapsed_us > 0) {
        work_metrics_->completed_after_cancellation.fetch_add(
            1, std::memory_order_relaxed);
        work_metrics_->post_cancel_work_time_us.fetch_add(
            static_cast<std::uint64_t>(elapsed_us), std::memory_order_relaxed);
      }
    }
  }

  void start_dependency();
  void schedule_delay() {
    if (delay_ == std::chrono::microseconds::zero()) {
      complete(honor_cancellation_ && cancelled_.load(std::memory_order_acquire)
                 ? grpc::Status(grpc::StatusCode::CANCELLED, "server call cancelled")
                 : grpc::Status::OK);
      return;
    }
    bool cancelled = false;
    {
      std::lock_guard lock(alarm_mutex_);
      cancelled = honor_cancellation_ &&
                  cancelled_.load(std::memory_order_acquire);
      if (!cancelled) {
        std::weak_ptr<ACallState> weak_self =
            std::static_pointer_cast<ACallState>(shared_from_this());
        alarm_.Set(monotonic_deadline(delay_), [weak_self](bool ok) {
          auto state = weak_self.lock();
          if (!state) return;
          if (state->honor_cancellation_ &&
              state->cancelled_.load(std::memory_order_acquire)) {
            state->complete({grpc::StatusCode::CANCELLED, "server call cancelled"});
            return;
          }
          state->complete(ok ? grpc::Status::OK
                           : grpc::Status(grpc::StatusCode::CANCELLED,
                                          "server call cancelled"));
        });
      }
    }
    if (cancelled) {
      complete({grpc::StatusCode::CANCELLED, "server call cancelled"});
    }
  }

  grpc::CallbackServerContext* context_;
  const artc::v1::WorkRequest request_;
  artc::v1::WorkResponse* response_;
  std::string replica_id_;
  std::uint64_t delay_per_work_unit_us_;
  std::shared_ptr<artc::v1::Traffic::Stub> dependency_stub_;
  grpc::Status injected_status_;
  bool honor_cancellation_;
  std::shared_ptr<ServiceAWorkMetrics> work_metrics_;
  std::chrono::microseconds delay_{0};
  std::mutex work_mutex_;
  std::optional<std::chrono::steady_clock::time_point> cancel_time_;
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
    cancelled = honor_cancellation_ &&
                cancelled_.load(std::memory_order_acquire);
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
        control::ControllerConfig controller_config,
        std::unordered_map<std::string, MethodPolicy> configured_method_policies,
        AttemptRuntimeConfig configured_attempts)
      : selector(routing::make_selector(selected_policy, seed, smoothing)),
        secondary_selector(routing::make_selector(selected_policy, seed + 1, smoothing)),
        policy(selected_policy),
        method_policies(std::move(configured_method_policies)),
        attempt_config(configured_attempts),
        hedge_budget(attempt_config.hedge_budget),
        retry_budget(attempt_config.retry_budget),
        retry_rng(attempt_config.jitter_seed) {
    if (configs.empty()) throw std::invalid_argument("router requires at least one replica");
    if (attempt_config.max_total_attempts == 0 ||
        attempt_config.max_total_attempts > kHardMaxTotalAttempts ||
        attempt_config.max_active_attempts == 0 ||
        attempt_config.max_active_attempts > 2 ||
        attempt_config.max_active_attempts > attempt_config.max_total_attempts ||
        attempt_config.minimum_attempt_budget <= std::chrono::microseconds::zero() ||
        attempt_config.minimum_attempt_budget > std::chrono::seconds(60)) {
      throw std::invalid_argument("invalid Phase 3 attempt limits");
    }
    for (const auto& [method, method_policy] : method_policies) {
      if (method.empty()) throw std::invalid_argument("method policy key must not be empty");
      validate(method_policy);
      if (method_policy.max_total_attempts > attempt_config.max_total_attempts) {
        throw std::invalid_argument("method policy exceeds runtime attempt limit");
      }
      if (method_policy.hedging_enabled && attempt_config.max_active_attempts < 2) {
        throw std::invalid_argument("hedging requires max_active_attempts of at least two");
      }
    }
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
        if (std::any_of(method_policies.begin(), method_policies.end(),
                        [](const auto& entry) {
                          return entry.second.hedging_enabled || entry.second.retry_enabled;
                        })) {
          throw std::invalid_argument(
              "bounded secondary attempts require Phase 2 logical admission");
        }
        return;
    }
    if (std::any_of(method_policies.begin(), method_policies.end(),
                    [](const auto& entry) { return entry.second.hedging_enabled; }) &&
        states.size() < 2) {
      throw std::invalid_argument("hedging requires at least two replicas");
    }
    controller = std::make_unique<control::Phase2Controller>(controller_config, states);
    if (controller_config.adaptive_selector) {
      adaptive_selector = std::make_unique<control::AdaptiveSelector>(controller_config);
    }
    controller->start();
  }

  [[nodiscard]] bool phase2_enabled() const noexcept { return controller != nullptr; }

  [[nodiscard]] MethodPolicy method_policy(std::string_view method) const {
    return resolve_method_policy(method, method_policies);
  }

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

  routing::ReplicaLease reserve_secondary(
      std::size_t* backend_index,
      std::shared_ptr<const control::ControllerSnapshot>* snapshot,
      std::span<const std::size_t> excluded) {
    if (controller) *snapshot = controller->snapshot();
    if (adaptive_selector) {
      const auto selection = adaptive_selector->select_secondary(**snapshot, states, excluded);
      if (!selection.selected) throw NoHealthyReplica();
      *backend_index = *selection.selected;
      return routing::ReplicaLease(states[*backend_index]);
    }
    std::vector<std::shared_ptr<routing::ReplicaState>> candidates;
    candidates.reserve(states.size());
    for (std::size_t index = 0; index < states.size(); ++index) {
      if (std::find(excluded.begin(), excluded.end(), index) != excluded.end()) continue;
      if (controller && *snapshot && index < (*snapshot)->replicas.size() &&
          (*snapshot)->replicas[index].health != routing::HealthState::kHealthy) continue;
      candidates.push_back(states[index]);
    }
    if (candidates.empty()) throw NoHealthyReplica();
    auto lease = secondary_selector->select(candidates);
    const auto target = lease.replica();
    const auto found = std::find_if(backends.begin(), backends.end(), [&](const Backend& backend) {
      return backend.state == target;
    });
    if (found == backends.end()) throw std::logic_error("selector returned unknown replica");
    *backend_index = static_cast<std::size_t>(std::distance(backends.begin(), found));
    return lease;
  }

  std::unique_ptr<routing::Selector> selector;
  // Secondary selection must not advance state owned by primary routing.
  std::unique_ptr<routing::Selector> secondary_selector;
  routing::Policy policy;
  std::vector<std::shared_ptr<routing::ReplicaState>> states;
  std::vector<Backend> backends;
  std::unordered_map<std::string, MethodPolicy> method_policies;
  AttemptRuntimeConfig attempt_config;
  HedgeBudget hedge_budget;
  RetryBudget retry_budget;
  std::mutex retry_rng_mutex;
  std::mt19937_64 retry_rng;
  // ponytail: serialize the brief dispatch commit with shutdown; replace only if benchmarks expose contention.
  std::mutex dispatch_mutex;
  std::atomic<bool> dispatch_closed{false};
  std::stop_source shutdown_source;
  struct Metrics {
    std::atomic<std::uint64_t> logical_requests{0};
    std::atomic<std::uint64_t> logical_terminals{0};
    std::array<std::atomic<std::uint64_t>, 3> attempts{};
    std::array<std::atomic<std::uint64_t>, 3> completions{};
    std::array<std::atomic<std::uint64_t>, 3> winners{};
    std::atomic<std::uint64_t> cancelled_attempts{0};
    std::array<std::atomic<std::uint64_t>,
               static_cast<std::size_t>(AttemptCancellationReason::kCount)>
        cancellations_by_reason{};
    std::atomic<std::uint64_t> wasted_attempt_time_us{0};
    std::atomic<std::uint64_t> censored_attempts{0};
    std::atomic<std::uint64_t> hedge_started{0};
    std::atomic<std::uint64_t> hedge_budget_denied{0};
    std::atomic<std::uint64_t> hedge_deadline_denied{0};
    std::atomic<std::uint64_t> hedge_overload_denied{0};
    std::atomic<std::uint64_t> hedge_no_target{0};
    std::atomic<std::uint64_t> hedge_attempt_limit{0};
    std::atomic<std::uint64_t> hedge_dispatch_errors{0};
    std::atomic<std::uint64_t> retry_started{0};
    std::atomic<std::uint64_t> retry_budget_denied{0};
    std::atomic<std::uint64_t> retry_deadline_denied{0};
    std::atomic<std::uint64_t> retry_not_retryable{0};
    std::atomic<std::uint64_t> retry_no_target{0};
    std::atomic<std::uint64_t> retry_attempt_limit{0};
    std::atomic<std::uint64_t> retry_same_replica{0};
    std::atomic<std::uint64_t> pending_hedge_timers{0};
    std::atomic<std::uint64_t> pending_retry_timers{0};
    std::atomic<std::uint64_t> active_attempts{0};
  } metrics;
  std::shared_ptr<BackendCallbackDrain> callback_drain{
      std::make_shared<BackendCallbackDrain>()};

  std::shared_ptr<BackendCallbackDrain> register_backend_callback() {
    callback_drain->register_callback();
    return callback_drain;
  }

  [[nodiscard]] bool wait_for_backend_callbacks(
      std::chrono::steady_clock::time_point deadline) const {
    return callback_drain->wait_until(deadline);
  }
  std::unique_ptr<control::AdaptiveSelector> adaptive_selector;
  std::unique_ptr<control::Phase2Controller> controller;
  std::atomic<std::uint64_t> decision_sequence{0};
};

namespace {

struct LogicalRequest {
  control::RequestContext context;
  MethodPolicy policy;
  artc::v1::WorkRequest request;
};

void saturating_add(std::atomic<std::uint64_t>& value, std::uint64_t amount) noexcept {
  auto current = value.load(std::memory_order_relaxed);
  while (current != std::numeric_limits<std::uint64_t>::max()) {
    const auto next = amount > std::numeric_limits<std::uint64_t>::max() - current
                          ? std::numeric_limits<std::uint64_t>::max()
                          : current + amount;
    if (value.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                    std::memory_order_relaxed)) {
      return;
    }
  }
}

std::size_t attempt_kind_index(AttemptKind kind) noexcept {
  return static_cast<std::size_t>(kind);
}

const char* attempt_kind_name(AttemptKind kind) noexcept {
  switch (kind) {
    case AttemptKind::kPrimary: return "primary";
    case AttemptKind::kHedge: return "hedge";
    case AttemptKind::kRetry: return "retry";
  }
  return "unknown";
}

std::chrono::microseconds ceil_microseconds(std::chrono::nanoseconds duration) noexcept {
  if (duration <= std::chrono::nanoseconds::zero()) return std::chrono::microseconds::zero();
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(duration);
  if (std::chrono::duration_cast<std::chrono::nanoseconds>(result) < duration &&
      result < std::chrono::microseconds::max()) {
    ++result;
  }
  return result;
}

class AttemptManager final : public CompletionState {
 public:
  struct BackendAttempt;

  AttemptManager(grpc::ServerUnaryReactor* reactor,
                 grpc::CallbackServerContext* context,
                 artc::v1::WorkResponse* response,
                 std::shared_ptr<RouterService::State> owner,
                 LogicalRequest logical, routing::ReplicaLease primary_lease,
                 control::AdmissionPermit permit, std::size_t backend_index,
                 std::string decision_metadata)
      : CompletionState(reactor),
        context_(context),
        response_(response),
        owner_(std::move(owner)),
        logical_(std::move(logical)),
        primary_lease_(std::move(primary_lease)),
        permit_(std::move(permit)),
        primary_backend_index_(backend_index),
        decision_metadata_(std::move(decision_metadata)),
        total_attempt_limit_(std::min(logical_.policy.max_total_attempts,
                                      owner_->attempt_config.max_total_attempts)) {
    if (const auto replica = primary_lease_.replica()) primary_replica_id_ = replica->id;
    saturating_add(owner_->metrics.logical_requests, 1);
  }

  struct BackendAttempt final : DependencyRequest {
    explicit BackendAttempt(routing::ReplicaLease attempt_lease)
        : lease(std::move(attempt_lease)) {}

    std::uint64_t id{0};
    AttemptKind kind{AttemptKind::kPrimary};
    AttemptState state{AttemptState::kCreated};
    std::size_t backend_index{0};
    std::string replica_id;
    control::SteadyTime started_at{};
    control::SteadyTime deadline{};
    control::SteadyTime cancellation_requested_at{};
    routing::ReplicaLease lease;
    grpc::Status status;
    bool cancel_requested{false};
    AttemptCancellationReason cancellation_reason{AttemptCancellationReason::kInternal};
    bool censored_by_winner{false};
    bool accounted{false};
  };

  void start() noexcept {
    try {
      const auto self = std::static_pointer_cast<AttemptManager>(shared_from_this());
      const std::weak_ptr<AttemptManager> weak_self = self;
      shutdown_callback_.emplace(
          owner_->shutdown_source.get_token(),
          std::function<void()>([weak_self] {
            if (const auto manager = weak_self.lock()) manager->shutdown();
          }));
      CompletionPlan plan;
      {
        std::lock_guard lock(mutex_);
        if (state_ != LogicalState::kActive) return;
        if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
            owner_->shutdown_source.stop_requested()) {
          finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
                        control::RequestOutcome::kCancelled, nullptr, &plan,
                        AttemptCancellationReason::kShutdown);
        } else if (context_->IsCancelled()) {
          finish_cancelled_locked(&plan);
        } else {
          schedule_deadline_locked(weak_self);
          const auto result = start_attempt_locked(
              AttemptKind::kPrimary, primary_backend_index_, std::move(primary_lease_),
              std::chrono::nanoseconds::zero(), static_cast<NoBudget*>(nullptr), &plan);
          static_cast<void>(result);
        }
      }
      execute_plan(std::move(plan));
    } catch (const std::exception& error) {
      fail_internal(error.what());
    } catch (...) {
      fail_internal("unexpected attempt setup failure");
    }
  }

  void cancel() override {
    CompletionPlan plan;
    {
      std::lock_guard lock(mutex_);
      if (state_ != LogicalState::kActive) return;
      const bool deadline_expired = logical_deadline_expired_locked();
      if (deadline_expired) {
        finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                       "logical request deadline expired"},
                      control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
                      AttemptCancellationReason::kDeadline);
      } else {
        finish_locked({grpc::StatusCode::CANCELLED, "router request cancelled"},
                      control::RequestOutcome::kCancelled, nullptr, &plan,
                      AttemptCancellationReason::kCaller);
      }
    }
    execute_plan(std::move(plan));
  }

  void shutdown() noexcept {
    CompletionPlan plan;
    {
      std::lock_guard lock(mutex_);
      if (state_ != LogicalState::kActive) return;
      finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
                    control::RequestOutcome::kCancelled, nullptr, &plan,
                    AttemptCancellationReason::kShutdown);
    }
    execute_plan(std::move(plan));
  }

  void backend_done(const std::shared_ptr<BackendAttempt>& attempt,
                    const grpc::Status& status) noexcept {
    const auto now = control::SteadyClock::now();
    CompletionPlan plan;
    control::RequestOutcome attempt_outcome = control::RequestOutcome::kFailure;
    bool was_terminal = false;
    bool record_censored = false;
    double censored_lower_bound_us = 0.0;
    {
      std::lock_guard lock(mutex_);
      if (attempt->accounted) return;
      if (active_attempts_ == 0) std::terminate();
      was_terminal = state_ == LogicalState::kCompleted;
      const auto decision_now = control::SteadyClock::now();
      const bool deadline_expired =
          !was_terminal && logical_deadline_expired_at(decision_now);
      attempt->accounted = true;
      attempt->status = status;
      --active_attempts_;
      owner_->metrics.active_attempts.fetch_sub(1, std::memory_order_relaxed);
      saturating_add(owner_->metrics.completions[attempt_kind_index(attempt->kind)], 1);

      if (status.ok()) {
        attempt->state = AttemptState::kSucceeded;
        const bool request_timed_out =
            (!was_terminal && deadline_expired) ||
            (was_terminal && logical_.context.terminal_outcome ==
                                 control::RequestOutcome::kDeadlineMiss);
        attempt_outcome = request_timed_out
                              ? control::RequestOutcome::kDeadlineMiss
                              : control::RequestOutcome::kSuccess;
      } else if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        attempt->state = AttemptState::kTimedOut;
        attempt_outcome = control::RequestOutcome::kTimeout;
      } else if (status.error_code() == grpc::StatusCode::CANCELLED &&
                 (attempt->cancel_requested || (!was_terminal && deadline_expired))) {
        attempt->state = AttemptState::kCancelled;
        if (!attempt->cancel_requested && deadline_expired) {
          attempt->cancellation_reason = AttemptCancellationReason::kDeadline;
          attempt->cancellation_requested_at = decision_now;
        }
        attempt_outcome = attempt->cancellation_reason ==
                                  AttemptCancellationReason::kDeadline
                              ? control::RequestOutcome::kTimeout
                              : control::RequestOutcome::kCancelled;
        record_censored = attempt->censored_by_winner;
        if (record_censored) {
          censored_lower_bound_us = attempt_latency_us(
              attempt->cancellation_requested_at, attempt->started_at);
        }
      } else {
        attempt->state = AttemptState::kFailed;
        attempt_outcome = control::RequestOutcome::kFailure;
      }

      if (!was_terminal) {
        if (deadline_expired) {
          finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                         "logical request deadline expired"},
                        control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
                        AttemptCancellationReason::kDeadline);
        } else if (status.ok()) {
          finish_locked(grpc::Status::OK, control::RequestOutcome::kSuccess,
                        attempt.get(), &plan);
        } else {
          if (attempt->kind == AttemptKind::kPrimary) cancel_hedge_locked();
          advance_after_failure_locked(&plan);
        }
      } else {
        record_wasted_time_locked(*attempt, now);
      }
      release_permit_if_drained_locked();
    }

    if (owner_->controller) {
      const auto observed_at =
          status.error_code() == grpc::StatusCode::CANCELLED &&
                  attempt->cancellation_reason == AttemptCancellationReason::kDeadline
              ? attempt->cancellation_requested_at
              : now;
      const double latency_us = attempt_latency_us(observed_at, attempt->started_at);
      static_cast<void>(owner_->controller->record_attempt_completion(
          attempt->backend_index, observed_at, latency_us, attempt_outcome));
    } else if (status.ok()) {
      owner_->selector->record_latency(attempt->lease.replica(), attempt_latency_us(
          now, attempt->started_at));
    }
    if (record_censored && attempt->lease.replica()->record_censored_latency_lower_bound(
                               censored_lower_bound_us,
                               attempt->cancellation_requested_at)) {
      saturating_add(owner_->metrics.censored_attempts, 1);
    }
    attempt->lease.release();
    execute_plan(std::move(plan));
  }

 private:
  enum class LogicalState : std::uint8_t { kActive, kCompleted };
  enum class StartResult : std::uint8_t {
    kStarted,
    kExpired,
    kDeadlineDenied,
    kBudgetDenied,
    kAtLimit,
  };
  struct NoBudget {};
  class BackendReactor final : public grpc::ClientUnaryReactor {
   public:
    BackendReactor(std::shared_ptr<AttemptManager> manager,
                   std::shared_ptr<BackendAttempt> attempt)
        : manager_(std::move(manager)), attempt_(std::move(attempt)) {}

    ~BackendReactor() override {
      auto drain = std::move(drain_);
      attempt_.reset();
      manager_.reset();
      if (drain) drain->callback_destroyed();
    }

    void register_for_drain() {
      drain_ = manager_->register_backend_reactor();
    }

    void OnDone(const grpc::Status& status) override {
      manager_->backend_done(attempt_, status);
      delete this;
    }

   private:
    std::shared_ptr<AttemptManager> manager_;
    std::shared_ptr<BackendAttempt> attempt_;
    std::shared_ptr<BackendCallbackDrain> drain_;
  };

  struct CompletionPlan {
    bool finish{false};
    grpc::Status status;
    control::RequestOutcome outcome{control::RequestOutcome::kFailure};
    std::uint32_t attempt_count{0};
    std::array<std::uint32_t, 3> attempts_by_kind{};
    std::optional<AttemptKind> winner_kind;
    std::string winner_replica;
    std::array<std::shared_ptr<BackendAttempt>, kHardMaxTotalAttempts>
        cancel_attempts{};
    std::size_t cancel_count{0};
  };

  [[nodiscard]] double attempt_latency_us(control::SteadyTime completed,
                                          control::SteadyTime started) const noexcept {
    return std::chrono::duration<double, std::micro>(completed - started).count();
  }

  [[nodiscard]] bool logical_deadline_expired_at(control::SteadyTime now) const noexcept {
    return owner_->controller && now >= logical_.context.effective_deadline;
  }

  [[nodiscard]] bool logical_deadline_expired_locked() const noexcept {
    return logical_deadline_expired_at(control::SteadyClock::now());
  }

  void schedule_deadline_locked(const std::weak_ptr<AttemptManager>& weak_self) {
    if (!owner_->controller ||
        logical_.context.effective_deadline == control::SteadyTime::max()) return;
    const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
    if (remaining <= std::chrono::nanoseconds::zero()) return;
    deadline_alarm_.Set(monotonic_deadline(ceil_microseconds(remaining)),
                        [weak_self](bool ok) {
                          if (ok) {
                            if (const auto manager = weak_self.lock()) {
                              manager->on_deadline_alarm();
                            }
                          }
                        });
  }

  void on_deadline_alarm() noexcept {
    CompletionPlan plan;
    {
      std::lock_guard lock(mutex_);
      if (state_ != LogicalState::kActive) return;
      const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
      if (remaining > std::chrono::nanoseconds::zero()) {
        const auto weak_self = std::weak_ptr<AttemptManager>(
            std::static_pointer_cast<AttemptManager>(shared_from_this()));
        deadline_alarm_.Set(monotonic_deadline(ceil_microseconds(remaining)),
                            [weak_self](bool ok) {
                              if (ok) {
                                if (const auto manager = weak_self.lock()) {
                                  manager->on_deadline_alarm();
                                }
                              }
                            });
        return;
      }
      finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                     "logical request deadline expired"},
                    control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
                    AttemptCancellationReason::kDeadline);
    }
    execute_plan(std::move(plan));
  }

  [[nodiscard]] std::chrono::nanoseconds predicted_attempt_budget(
      std::optional<std::size_t> excluded = std::nullopt) const noexcept {
    auto predicted = std::chrono::duration_cast<std::chrono::nanoseconds>(
        owner_->attempt_config.minimum_attempt_budget);
    if (!owner_->controller) return predicted;
    const auto snapshot = owner_->controller->snapshot();
    if (!snapshot) return predicted;
    double fastest_us = std::numeric_limits<double>::infinity();
    bool candidate_found = false;
    for (std::size_t index = 0; index < snapshot->replicas.size(); ++index) {
      if ((excluded && index == *excluded) ||
          snapshot->replicas[index].health == routing::HealthState::kUnavailable) continue;
      candidate_found = true;
      const auto& replica = snapshot->replicas[index];
      const double estimate = replica.latency_samples != 0 &&
                                      std::isfinite(replica.latency_p95_us) &&
                                      replica.latency_p95_us > 0.0
                                  ? replica.latency_p95_us
                                  : owner_->controller->config().cold_start_latency_us;
      fastest_us = std::min(fastest_us, estimate);
    }
    if (!candidate_found || !std::isfinite(fastest_us)) return predicted;
    constexpr double kMaximumMicroseconds = 60'000'000.0;
    const auto bounded_us = static_cast<std::int64_t>(std::ceil(
        std::clamp(fastest_us, 0.0, kMaximumMicroseconds)));
    return std::max(predicted,
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::microseconds(bounded_us)));
  }

  [[nodiscard]] std::chrono::nanoseconds predicted_target_budget(
      std::size_t target) const noexcept {
    auto predicted = std::chrono::duration_cast<std::chrono::nanoseconds>(
        owner_->attempt_config.minimum_attempt_budget);
    if (!owner_->controller) return predicted;
    const auto snapshot = owner_->controller->snapshot();
    if (!snapshot || target >= snapshot->replicas.size()) return predicted;
    const auto& replica = snapshot->replicas[target];
    const double estimate = replica.latency_samples != 0 &&
                                    std::isfinite(replica.latency_p95_us) &&
                                    replica.latency_p95_us > 0.0
                                ? replica.latency_p95_us
                                : owner_->controller->config().cold_start_latency_us;
    constexpr double kMaximumMicroseconds = 60'000'000.0;
    const auto bounded_us = static_cast<std::int64_t>(std::ceil(
        std::clamp(estimate, 0.0, kMaximumMicroseconds)));
    return std::max(predicted,
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::microseconds(bounded_us)));
  }

  [[nodiscard]] std::chrono::microseconds hedge_delay() const noexcept {
    const auto snapshot = owner_->controller->snapshot();
    if (!snapshot || primary_backend_index_ >= snapshot->replicas.size()) {
      return hedge_delay_for_p95(logical_.policy, std::nullopt);
    }
    const auto& primary = snapshot->replicas[primary_backend_index_];
    if (primary.latency_samples == 0 || !std::isfinite(primary.latency_p95_us) ||
        primary.latency_p95_us <= 0.0) {
      return hedge_delay_for_p95(logical_.policy, std::nullopt);
    }
    const auto bounded_us = static_cast<std::int64_t>(std::ceil(
        std::clamp(primary.latency_p95_us, 0.0, 60'000'000.0)));
    return hedge_delay_for_p95(logical_.policy, std::chrono::microseconds(bounded_us));
  }

  [[nodiscard]] bool overloaded_for_hedging_locked() const noexcept {
    const auto snapshot = owner_->controller->snapshot();
    const auto admission = owner_->controller->admission_snapshot();
    // The current logical request owns one route permit; don't mistake that
    // permit alone for cluster-wide pressure when the route limit is one.
    if (!snapshot || !admission.accepting || admission.limit == 0 ||
        (admission.inflight > 1 && admission.inflight >= admission.limit)) return true;
    const double target_us = static_cast<double>(
        owner_->controller->config().aimd.target_latency.count());
    const auto minimum_samples =
        owner_->controller->config().health.minimum_latency_samples;
    bool observed_latency = false;
    for (const auto& replica : snapshot->replicas) {
      if (replica.health == routing::HealthState::kUnavailable) continue;
      double estimate = replica.latency_p95_us > 0.0
                            ? replica.latency_p95_us
                            : replica.latency_ewma_us;
      if (replica.censored_latency_samples >= minimum_samples) {
        estimate = std::max(estimate,
                            replica.censored_latency_p95_lower_bound_us);
      }
      if (!std::isfinite(estimate) || estimate <= 0.0) return false;
      observed_latency = true;
      if (estimate <= target_us) return false;
    }
    return observed_latency;
  }

  void schedule_hedge_locked(const std::weak_ptr<AttemptManager>& weak_self,
                             const BackendAttempt& primary) {
    if (!logical_.policy.hedging_enabled || hedge_started_ || !owner_->controller ||
        owner_->dispatch_closed.load(std::memory_order_acquire) ||
        owner_->shutdown_source.stop_requested() ||
        state_ != LogicalState::kActive || primary.state != AttemptState::kInFlight ||
        total_attempts_ >= total_attempt_limit_ ||
        active_attempts_ >= owner_->attempt_config.max_active_attempts) return;
    const auto delay = hedge_delay();
    const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
    const auto minimum = predicted_attempt_budget(primary.backend_index);
    const auto delay_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    if (!retry_delay_fits_deadline(remaining, delay_ns, minimum)) {
      saturating_add(owner_->metrics.hedge_deadline_denied, 1);
      return;
    }
    auto until_fire = delay;
    const auto elapsed = control::SteadyClock::now() - primary.started_at;
    if (elapsed > control::SteadyClock::duration::zero()) {
      const auto already = std::chrono::duration_cast<std::chrono::microseconds>(elapsed);
      until_fire = already >= delay ? std::chrono::microseconds::zero() : delay - already;
    }
    hedge_pending_ = true;
    hedge_alarm_.Set(monotonic_deadline(until_fire), [weak_self](bool ok) {
      if (ok) {
        if (const auto manager = weak_self.lock()) manager->on_hedge_alarm();
      }
    });
    saturating_add(owner_->metrics.pending_hedge_timers, 1);
  }

  void on_hedge_alarm() noexcept {
    CompletionPlan plan;
    try {
      std::lock_guard lock(mutex_);
      if (state_ != LogicalState::kActive || !hedge_pending_) return;
      clear_hedge_pending_locked(false);
      if (attempts_[0] == nullptr ||
          attempts_[0]->state != AttemptState::kInFlight) return;
      if (total_attempts_ >= total_attempt_limit_ ||
          active_attempts_ >= owner_->attempt_config.max_active_attempts) {
        saturating_add(owner_->metrics.hedge_attempt_limit, 1);
        return;
      }
      if (overloaded_for_hedging_locked()) {
        saturating_add(owner_->metrics.hedge_overload_denied, 1);
        return;
      }
      std::shared_ptr<const control::ControllerSnapshot> snapshot;
      std::size_t target = 0;
      std::optional<routing::ReplicaLease> lease;
      try {
        const std::array excluded{primary_backend_index_};
        lease.emplace(owner_->reserve_secondary(&target, &snapshot, excluded));
      } catch (const NoHealthyReplica&) {
        saturating_add(owner_->metrics.hedge_no_target, 1);
        return;
      }
      if (target == primary_backend_index_) {
        lease->release();
        saturating_add(owner_->metrics.hedge_no_target, 1);
        return;
      }
      if (overloaded_for_hedging_locked()) {
        lease->release();
        saturating_add(owner_->metrics.hedge_overload_denied, 1);
        return;
      }
      const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
      const auto required = predicted_target_budget(target);
      if (!retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(), required)) {
        lease->release();
        saturating_add(owner_->metrics.hedge_deadline_denied, 1);
        return;
      }
      const auto result = start_attempt_locked(AttemptKind::kHedge, target,
                                                std::move(*lease), required,
                                                &owner_->hedge_budget, &plan);
      if (result == StartResult::kBudgetDenied) {
        saturating_add(owner_->metrics.hedge_budget_denied, 1);
      } else if (result == StartResult::kDeadlineDenied) {
        saturating_add(owner_->metrics.hedge_deadline_denied, 1);
      } else if (result == StartResult::kAtLimit) {
        saturating_add(owner_->metrics.hedge_attempt_limit, 1);
      }
    } catch (...) {
      CompletionPlan fallback;
      {
        std::lock_guard lock(mutex_);
        saturating_add(owner_->metrics.hedge_dispatch_errors, 1);
        const bool primary_can_continue =
            state_ == LogicalState::kActive && attempts_[0] != nullptr &&
            attempts_[0]->state == AttemptState::kInFlight && active_attempts_ == 1;
        if (!primary_can_continue) {
          finish_locked({grpc::StatusCode::INTERNAL, "hedge dispatch failed"},
                        control::RequestOutcome::kFailure, nullptr, &fallback);
        }
      }
      execute_plan(std::move(fallback));
      return;
    }
    execute_plan(std::move(plan));
  }

  [[nodiscard]] std::chrono::milliseconds retry_jitter() {
    const auto maximum = logical_.policy.retry_jitter_max.count();
    if (maximum <= 0) return std::chrono::milliseconds::zero();
    std::lock_guard lock(owner_->retry_rng_mutex);
    std::uniform_int_distribution<std::int64_t> distribution(0, maximum);
    return std::chrono::milliseconds(distribution(owner_->retry_rng));
  }

  [[nodiscard]] const BackendAttempt* latest_failure_locked() const noexcept {
    if (total_attempts_ == 0) return nullptr;
    const std::size_t begin = retries_started_ == 0 ? 0 : total_attempts_ - 1;
    for (std::size_t index = total_attempts_; index > begin; --index) {
      const auto& attempt = attempts_[index - 1];
      if (attempt && attempt->state != AttemptState::kSucceeded) return attempt.get();
    }
    return nullptr;
  }

  void advance_after_failure_locked(CompletionPlan* plan) {
    if (state_ != LogicalState::kActive || active_attempts_ != 0) return;
    cancel_hedge_locked();
    if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
        owner_->shutdown_source.stop_requested()) {
      finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
                    control::RequestOutcome::kCancelled, nullptr, plan,
                    AttemptCancellationReason::kShutdown);
      return;
    }
    const std::size_t group_begin = retries_started_ == 0 ? 0 : total_attempts_ - 1;
    const BackendAttempt* latest = nullptr;
    const BackendAttempt* permanent = nullptr;
    for (std::size_t index = group_begin; index < total_attempts_; ++index) {
      const auto& attempt = attempts_[index];
      if (!attempt || attempt->state == AttemptState::kSucceeded) continue;
      if (!latest || attempt->id > latest->id) latest = attempt.get();
      if (!is_retryable(logical_.policy, attempt->status.error_code()) &&
          (!permanent || attempt->id < permanent->id)) {
        permanent = attempt.get();
      }
    }
    if (!latest) return;
    if (permanent) {
      if (logical_.policy.retry_enabled) {
        saturating_add(owner_->metrics.retry_not_retryable, 1);
      }
      const auto outcome = permanent->status.error_code() ==
                                   grpc::StatusCode::DEADLINE_EXCEEDED
                               ? control::RequestOutcome::kTimeout
                               : control::RequestOutcome::kFailure;
      finish_locked(permanent->status, outcome, nullptr, plan);
      return;
    }
    if (!logical_.policy.retry_enabled ||
        logical_.policy.idempotency != Idempotency::kIdempotent ||
        retries_started_ >= logical_.policy.max_retries ||
        total_attempts_ >= total_attempt_limit_) {
      if (logical_.policy.retry_enabled &&
          logical_.policy.idempotency == Idempotency::kIdempotent &&
          (retries_started_ >= logical_.policy.max_retries ||
           total_attempts_ >= total_attempt_limit_)) {
        saturating_add(owner_->metrics.retry_attempt_limit, 1);
      }
      const auto outcome = latest->status.error_code() ==
                                   grpc::StatusCode::DEADLINE_EXCEEDED
                               ? control::RequestOutcome::kTimeout
                               : control::RequestOutcome::kFailure;
      finish_locked(latest->status, outcome, nullptr, plan);
      return;
    }

    const auto backoff = retry_backoff_delay(
        logical_.policy, retries_started_, retry_jitter());
    const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
    const auto required = predicted_attempt_budget(latest->backend_index);
    const auto backoff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(backoff);
    if (!retry_delay_fits_deadline(remaining, backoff_ns, required)) {
      saturating_add(owner_->metrics.retry_deadline_denied, 1);
      if (remaining <= std::chrono::nanoseconds::zero()) {
        finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                       "logical request deadline expired before retry"},
                      control::RequestOutcome::kDeadlineMiss, nullptr, plan,
                      AttemptCancellationReason::kDeadline);
      } else {
        finish_locked(latest->status,
                      control::RequestOutcome::kFailure, nullptr, plan);
      }
      return;
    }

    retry_failure_id_ = latest->id;
    retry_pending_ = true;
    const auto weak_self = std::weak_ptr<AttemptManager>(
        std::static_pointer_cast<AttemptManager>(shared_from_this()));
    retry_alarm_.Set(monotonic_deadline(backoff), [weak_self](bool ok) {
      if (ok) {
        if (const auto manager = weak_self.lock()) manager->on_retry_alarm();
      }
    });
    saturating_add(owner_->metrics.pending_retry_timers, 1);
  }

  void on_retry_alarm() noexcept {
    CompletionPlan plan;
    try {
      std::lock_guard lock(mutex_);
      if (state_ != LogicalState::kActive || !retry_pending_) return;
      clear_retry_pending_locked(false);
      if (logical_deadline_expired_locked()) {
      finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                     "logical request deadline expired before retry"},
                    control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
                    AttemptCancellationReason::kDeadline);
      } else if (retry_failure_id_ == 0 || retry_failure_id_ > total_attempts_) {
        finish_locked({grpc::StatusCode::INTERNAL, "retry state lost its failure"},
                      control::RequestOutcome::kFailure, nullptr, &plan);
      } else {
        const auto& failed = attempts_[retry_failure_id_ - 1];
        if (!failed || !is_retryable(logical_.policy, failed->status.error_code()) ||
            active_attempts_ != 0 || total_attempts_ >= total_attempt_limit_ ||
            retries_started_ >= logical_.policy.max_retries) {
          if (failed && is_retryable(logical_.policy, failed->status.error_code()) &&
              (total_attempts_ >= total_attempt_limit_ ||
               retries_started_ >= logical_.policy.max_retries)) {
            saturating_add(owner_->metrics.retry_attempt_limit, 1);
          }
          const auto* latest = latest_failure_locked();
          if (latest) {
            finish_locked(latest->status, control::RequestOutcome::kFailure,
                          nullptr, &plan);
          } else {
            finish_locked({grpc::StatusCode::INTERNAL,
                           "retry timer fired without a failed attempt"},
                          control::RequestOutcome::kFailure, nullptr, &plan);
          }
        } else {
          std::shared_ptr<const control::ControllerSnapshot> snapshot;
          std::size_t target = 0;
          std::optional<routing::ReplicaLease> lease;
          bool same_replica_fallback = false;
          bool have_target = false;
          std::array<std::size_t, kHardMaxTotalAttempts> excluded{};
          std::size_t excluded_count = 0;
          for (std::size_t index = 0; index < total_attempts_; ++index) {
            if (!attempts_[index]) continue;
            const auto replica_index = attempts_[index]->backend_index;
            const auto end = excluded.begin() +
                             static_cast<std::ptrdiff_t>(excluded_count);
            if (std::find(excluded.begin(), end, replica_index) == end) {
              excluded[excluded_count++] = replica_index;
            }
          }
          try {
            lease.emplace(owner_->reserve_secondary(
                &target, &snapshot,
                std::span<const std::size_t>(excluded.data(), excluded_count)));
            have_target = true;
          } catch (const NoHealthyReplica&) {
            if (!logical_.policy.allow_same_replica_retry) {
              saturating_add(owner_->metrics.retry_no_target, 1);
              finish_locked(failed->status, control::RequestOutcome::kFailure,
                            nullptr, &plan);
            } else {
              try {
                lease.emplace(owner_->reserve_secondary(
                    &target, &snapshot, std::span<const std::size_t>{}));
                const auto end = excluded.begin() +
                                 static_cast<std::ptrdiff_t>(excluded_count);
                same_replica_fallback =
                    std::find(excluded.begin(), end, target) != end;
                have_target = true;
              } catch (const NoHealthyReplica&) {
                saturating_add(owner_->metrics.retry_no_target, 1);
                finish_locked(failed->status, control::RequestOutcome::kFailure,
                              nullptr, &plan);
              }
            }
          }
          if (have_target) {
            const auto end = excluded.begin() +
                             static_cast<std::ptrdiff_t>(excluded_count);
            same_replica_fallback = same_replica_fallback ||
                                    std::find(excluded.begin(), end, target) != end;
            const auto required = predicted_target_budget(target);
            const auto remaining = logical_.context.remaining_budget(control::SteadyClock::now());
            if (!retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
                                           required)) {
              lease->release();
              saturating_add(owner_->metrics.retry_deadline_denied, 1);
              finish_locked(remaining <= std::chrono::nanoseconds::zero()
                                ? grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                                               "logical request deadline expired before retry")
                                : failed->status,
                            remaining <= std::chrono::nanoseconds::zero()
                                ? control::RequestOutcome::kDeadlineMiss
                                : control::RequestOutcome::kFailure,
                            nullptr, &plan);
            } else {
              const auto result = start_attempt_locked(AttemptKind::kRetry, target,
                                                        std::move(*lease), required,
                                                        &owner_->retry_budget, &plan);
              if (result == StartResult::kBudgetDenied) {
                saturating_add(owner_->metrics.retry_budget_denied, 1);
                finish_locked(failed->status, control::RequestOutcome::kFailure,
                              nullptr, &plan);
              } else if (result == StartResult::kDeadlineDenied) {
                saturating_add(owner_->metrics.retry_deadline_denied, 1);
                finish_locked(failed->status, control::RequestOutcome::kFailure,
                              nullptr, &plan);
              } else if (result == StartResult::kStarted && same_replica_fallback) {
                saturating_add(owner_->metrics.retry_same_replica, 1);
              } else if (result == StartResult::kAtLimit) {
                saturating_add(owner_->metrics.retry_attempt_limit, 1);
                finish_locked(failed->status, control::RequestOutcome::kFailure,
                              nullptr, &plan);
              }
            }
          }
        }
      }
    } catch (...) {
      fail_internal("retry dispatch failed");
      return;
    }
    execute_plan(std::move(plan));
  }

  template <typename Budget>
  StartResult start_attempt_locked(AttemptKind kind, std::size_t backend_index,
                                   routing::ReplicaLease lease,
                                   std::chrono::nanoseconds required_budget,
                                   Budget* budget, CompletionPlan* plan) {
    if (state_ != LogicalState::kActive ||
        total_attempts_ >= total_attempt_limit_ ||
        active_attempts_ >= owner_->attempt_config.max_active_attempts) {
      lease.release();
      return StartResult::kAtLimit;
    }
    if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
        owner_->shutdown_source.stop_requested()) {
      lease.release();
      finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
                    control::RequestOutcome::kCancelled, nullptr, plan,
                    AttemptCancellationReason::kShutdown);
      return StartResult::kExpired;
    }

    auto attempt = std::make_shared<BackendAttempt>(std::move(lease));
    attempt->id = static_cast<std::uint64_t>(total_attempts_ + 1);
    attempt->kind = kind;
    attempt->backend_index = backend_index;
    attempt->replica_id = attempt->lease.replica()->id;
    attempt->request = logical_.request;
    auto reactor = std::make_unique<BackendReactor>(
        std::static_pointer_cast<AttemptManager>(shared_from_this()), attempt);

    auto system_now = std::chrono::system_clock::now();
    auto steady_now = control::SteadyClock::now();
    auto remaining = logical_.context.remaining_budget(steady_now);
    if (owner_->controller && remaining <= std::chrono::nanoseconds::zero()) {
      attempt->lease.release();
      finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                     "logical request deadline expired before backend dispatch"},
                    control::RequestOutcome::kDeadlineMiss, nullptr, plan,
                    AttemptCancellationReason::kDeadline);
      return StartResult::kExpired;
    }
    if (required_budget > std::chrono::nanoseconds::zero() &&
        !retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
                                   required_budget)) {
      attempt->lease.release();
      return StartResult::kDeadlineDenied;
    }
    if (context_->IsCancelled()) {
      attempt->lease.release();
      finish_cancelled_locked(plan);
      return StartResult::kExpired;
    }

    // Serialize dispatch commitment with router shutdown. Shutdown closes this
    // gate before notifying managers, so an attempt either starts before that
    // boundary or is rejected without reaching the backend.
    std::lock_guard dispatch_lock(owner_->dispatch_mutex);
    if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
        owner_->shutdown_source.stop_requested()) {
      attempt->lease.release();
      finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
                    control::RequestOutcome::kCancelled, nullptr, plan,
                    AttemptCancellationReason::kShutdown);
      return StartResult::kExpired;
    }
    system_now = std::chrono::system_clock::now();
    steady_now = control::SteadyClock::now();
    remaining = logical_.context.remaining_budget(steady_now);
    if (owner_->controller && remaining <= std::chrono::nanoseconds::zero()) {
      attempt->lease.release();
      finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
                     "logical request deadline expired before backend dispatch"},
                    control::RequestOutcome::kDeadlineMiss, nullptr, plan,
                    AttemptCancellationReason::kDeadline);
      return StartResult::kExpired;
    }
    if (required_budget > std::chrono::nanoseconds::zero() &&
        !retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
                                   required_budget)) {
      attempt->lease.release();
      return StartResult::kDeadlineDenied;
    }
    if (context_->IsCancelled()) {
      attempt->lease.release();
      finish_cancelled_locked(plan);
      return StartResult::kExpired;
    }
    if constexpr (!std::is_same_v<Budget, NoBudget>) {
      if (budget != nullptr && !budget->try_consume(steady_now)) {
        attempt->lease.release();
        return StartResult::kBudgetDenied;
      }
    }

    attempt->started_at = steady_now;
    attempt->deadline = logical_.context.effective_deadline;
    attempt->state = AttemptState::kInFlight;
    if (owner_->controller) {
      attempt->context.set_deadline(
          logical_.context.downstream_deadline(steady_now, system_now));
    } else {
      attempt->context.set_deadline(context_->deadline());
    }
    attempts_[total_attempts_++] = attempt;
    ++active_attempts_;
    owner_->metrics.active_attempts.fetch_add(1, std::memory_order_relaxed);
    saturating_add(owner_->metrics.attempts[attempt_kind_index(kind)], 1);
    if (owner_->controller) owner_->controller->record_backend_attempt(backend_index);
    auto& stub = owner_->backends[backend_index].stub;
    reactor->register_for_drain();
    stub->async()->Execute(&attempt->context, &attempt->request,
                           &attempt->response, reactor.get());
    reactor->StartCall();
    static_cast<void>(reactor.release());

    if (kind == AttemptKind::kPrimary) {
      const auto weak_self = std::weak_ptr<AttemptManager>(
          std::static_pointer_cast<AttemptManager>(shared_from_this()));
      schedule_hedge_locked(weak_self, *attempt);
    } else if (kind == AttemptKind::kHedge) {
      hedge_started_ = true;
      saturating_add(owner_->metrics.hedge_started, 1);
    } else {
      ++retries_started_;
      saturating_add(owner_->metrics.retry_started, 1);
    }
    return StartResult::kStarted;
  }

  void finish_cancelled_locked(CompletionPlan* plan) {
    const bool deadline_expired = logical_deadline_expired_locked();
    finish_locked(deadline_expired
                      ? grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                                     "logical request deadline expired")
                      : grpc::Status(grpc::StatusCode::CANCELLED,
                                     "router request cancelled"),
                  deadline_expired ? control::RequestOutcome::kDeadlineMiss
                                   : control::RequestOutcome::kCancelled,
                  nullptr, plan,
                  deadline_expired ? AttemptCancellationReason::kDeadline
                                   : AttemptCancellationReason::kCaller);
  }

  bool finish_locked(grpc::Status status, control::RequestOutcome outcome,
                     const BackendAttempt* winner, CompletionPlan* plan,
                     AttemptCancellationReason cancel_reason =
                         AttemptCancellationReason::kInternal) {
    if (state_ != LogicalState::kActive) return false;
    if (outcome == control::RequestOutcome::kFailure &&
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      outcome = control::RequestOutcome::kTimeout;
    }
    state_ = LogicalState::kCompleted;
    saturating_add(owner_->metrics.logical_terminals, 1);
    completion_time_ = control::SteadyClock::now();
    logical_.context.terminal_outcome = outcome;
    if (owner_->controller) owner_->controller->record_logical_completion(outcome);
    cancel_hedge_locked();
    cancel_retry_locked();
    deadline_alarm_.Cancel();
    plan->finish = true;
    plan->status = std::move(status);
    plan->outcome = outcome;
    plan->attempt_count = static_cast<std::uint32_t>(total_attempts_);
    if (winner != nullptr) {
      response_->CopyFrom(winner->response);
      response_->set_backend_attempt_count(static_cast<std::uint32_t>(total_attempts_));
      plan->winner_kind = winner->kind;
      plan->winner_replica = winner->replica_id;
      saturating_add(owner_->metrics.winners[attempt_kind_index(winner->kind)], 1);
    }
    for (std::size_t index = 0; index < total_attempts_; ++index) {
      const auto& attempt = attempts_[index];
      if (attempt) ++plan->attempts_by_kind[attempt_kind_index(attempt->kind)];
      if (!attempt || attempt.get() == winner ||
          attempt->state != AttemptState::kInFlight || attempt->cancel_requested) continue;
      attempt->cancel_requested = true;
      attempt->cancellation_reason =
          winner != nullptr ? AttemptCancellationReason::kWinner : cancel_reason;
      attempt->cancellation_requested_at = completion_time_;
      attempt->censored_by_winner =
          winner != nullptr && outcome == control::RequestOutcome::kSuccess;
      const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
          completion_time_ - attempt->started_at).count();
      if (elapsed_us > 0) {
        saturating_add(owner_->metrics.wasted_attempt_time_us,
                       static_cast<std::uint64_t>(elapsed_us));
      }
      saturating_add(owner_->metrics.cancelled_attempts, 1);
      saturating_add(owner_->metrics.cancellations_by_reason[
                         cancellation_reason_index(attempt->cancellation_reason)], 1);
      plan->cancel_attempts[plan->cancel_count++] = attempt;
    }
    primary_lease_.release();
    release_permit_if_drained_locked();
    return true;
  }

  void release_permit_if_drained_locked() noexcept {
    if (state_ == LogicalState::kCompleted && active_attempts_ == 0) {
      permit_.release();
    }
  }

  void cancel_hedge_locked() {
    clear_hedge_pending_locked(true);
  }

  void cancel_retry_locked() {
    clear_retry_pending_locked(true);
  }

  void clear_hedge_pending_locked(bool cancel_alarm) noexcept {
    if (hedge_pending_) {
      hedge_pending_ = false;
      if (owner_->metrics.pending_hedge_timers.fetch_sub(
              1, std::memory_order_relaxed) == 0) std::terminate();
    }
    if (cancel_alarm) hedge_alarm_.Cancel();
  }

  void clear_retry_pending_locked(bool cancel_alarm) noexcept {
    if (retry_pending_) {
      retry_pending_ = false;
      if (owner_->metrics.pending_retry_timers.fetch_sub(
              1, std::memory_order_relaxed) == 0) std::terminate();
    }
    if (cancel_alarm) retry_alarm_.Cancel();
  }

  void record_wasted_time_locked(const BackendAttempt& attempt,
                                 control::SteadyTime completed_at) noexcept {
    if (completed_at <= completion_time_) return;
    const auto began = std::max(attempt.started_at, completion_time_);
    if (completed_at <= began) return;
    const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
        completed_at - began).count();
    if (microseconds > 0) {
      saturating_add(owner_->metrics.wasted_attempt_time_us,
                     static_cast<std::uint64_t>(microseconds));
    }
  }

  void execute_plan(CompletionPlan plan) noexcept {
    for (std::size_t index = 0; index < plan.cancel_count; ++index) {
      plan.cancel_attempts[index]->context.TryCancel();
    }
    if (!plan.finish) return;
    try {
      context_->AddTrailingMetadata(
          "artc-admission-result",
          control::admission_result_name(logical_.context.admission_result));
      context_->AddTrailingMetadata("artc-backend-attempts",
                                    std::to_string(plan.attempt_count));
      context_->AddTrailingMetadata("artc-primary-attempts",
                                    std::to_string(plan.attempts_by_kind[0]));
      context_->AddTrailingMetadata("artc-hedge-attempts",
                                    std::to_string(plan.attempts_by_kind[1]));
      context_->AddTrailingMetadata("artc-retry-attempts",
                                    std::to_string(plan.attempts_by_kind[2]));
      context_->AddTrailingMetadata("artc-cancelled-attempts",
                                    std::to_string(plan.cancel_count));
      context_->AddTrailingMetadata("artc-selected-replica", primary_replica_id_);
      if (plan.winner_kind) {
        context_->AddTrailingMetadata("artc-winning-attempt-kind",
                                      attempt_kind_name(*plan.winner_kind));
        context_->AddTrailingMetadata("artc-winning-replica", plan.winner_replica);
      }
      if (!decision_metadata_.empty()) {
        context_->AddTrailingMetadata("artc-decision", decision_metadata_);
      }
    } catch (...) {
      // Finish is the lifecycle guarantee; optional metadata may be omitted on allocation failure.
    }
    finish(std::move(plan.status));
  }

  void fail_internal(const char* message) noexcept {
    CompletionPlan plan;
    {
      std::lock_guard lock(mutex_);
      finish_locked({grpc::StatusCode::INTERNAL, message},
                    control::RequestOutcome::kFailure, nullptr, &plan);
    }
    execute_plan(std::move(plan));
  }

  void fail_internal(const std::string& message) noexcept {
    fail_internal(message.c_str());
  }

  std::shared_ptr<BackendCallbackDrain> register_backend_reactor() {
    return owner_->register_backend_callback();
  }

  grpc::CallbackServerContext* context_;
  artc::v1::WorkResponse* response_;
  std::shared_ptr<RouterService::State> owner_;
  LogicalRequest logical_;
  routing::ReplicaLease primary_lease_;
  control::AdmissionPermit permit_;
  std::size_t primary_backend_index_;
  std::string primary_replica_id_;
  std::string decision_metadata_;
  const std::uint32_t total_attempt_limit_;
  mutable std::mutex mutex_;
  LogicalState state_{LogicalState::kActive};
  std::array<std::shared_ptr<BackendAttempt>, kHardMaxTotalAttempts> attempts_{};
  std::size_t total_attempts_{0};
  std::size_t active_attempts_{0};
  std::uint32_t retries_started_{0};
  std::uint64_t retry_failure_id_{0};
  bool hedge_started_{false};
  bool hedge_pending_{false};
  bool retry_pending_{false};
  control::SteadyTime completion_time_{};
  grpc::Alarm hedge_alarm_;
  grpc::Alarm retry_alarm_;
  grpc::Alarm deadline_alarm_;
  std::optional<std::stop_callback<std::function<void()>>> shutdown_callback_;
};

}  // namespace

ServiceA::ServiceA(std::string replica_id, std::uint64_t delay_per_work_unit_us,
                   std::string dependency_address,
                   std::uint64_t unavailable_first_n,
                   bool honor_cancellation)
    : replica_id_(std::move(replica_id)),
      delay_per_work_unit_us_(delay_per_work_unit_us),
      unavailable_first_n_(unavailable_first_n),
      honor_cancellation_(honor_cancellation) {
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
  const auto sequence = execute_count_.fetch_add(1, std::memory_order_relaxed);
  work_metrics_->started.fetch_add(1, std::memory_order_relaxed);
  const grpc::Status injected_status = sequence < unavailable_first_n_
      ? grpc::Status(grpc::StatusCode::UNAVAILABLE, "injected transient unavailable")
      : grpc::Status::OK;
  auto state = std::make_shared<ACallState>(reactor, context, *request, response,
                                            replica_id_, delay_per_work_unit_us_,
                                            dependency_stub_, injected_status,
                                            honor_cancellation_, work_metrics_);
  reactor->bind(state);
  state->start();
  return reactor;
}

ServiceAWorkSnapshot ServiceA::work_snapshot() const noexcept {
  return {
      .started = work_metrics_->started.load(std::memory_order_relaxed),
      .completed = work_metrics_->completed.load(std::memory_order_relaxed),
      .cancellation_signals =
          work_metrics_->cancellation_signals.load(std::memory_order_relaxed),
      .completed_after_cancellation =
          work_metrics_->completed_after_cancellation.load(std::memory_order_relaxed),
      .post_cancel_work_time_us =
          work_metrics_->post_cancel_work_time_us.load(std::memory_order_relaxed),
  };
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
                             control::ControllerConfig controller_config,
                             std::unordered_map<std::string, MethodPolicy>
                                 method_policies,
                             AttemptRuntimeConfig attempt_config)
    : state_(std::make_shared<State>(std::move(replicas), policy, seed,
                                     ewma_smoothing, std::move(controller_config),
                                     std::move(method_policies),
                                     std::move(attempt_config))) {}

RouterService::~RouterService() { begin_shutdown(); }

void RouterService::begin_shutdown() noexcept {
  if (!state_) return;
  {
    std::lock_guard dispatch_lock(state_->dispatch_mutex);
    state_->dispatch_closed.store(true, std::memory_order_release);
  }
  state_->shutdown_source.request_stop();
  if (state_->controller) state_->controller->close_admission();
}

AttemptSnapshot RouterService::attempt_snapshot() const noexcept {
  AttemptSnapshot result;
  if (!state_) return result;
  const auto load = [](const std::atomic<std::uint64_t>& value) noexcept {
    return value.load(std::memory_order_relaxed);
  };
  result.logical_requests_total = load(state_->metrics.logical_requests);
  result.logical_terminal_transitions_total =
      load(state_->metrics.logical_terminals);
  for (std::size_t index = 0; index < result.backend_attempts_total.size(); ++index) {
    result.backend_attempts_total[index] = load(state_->metrics.attempts[index]);
    result.attempt_completions_total[index] = load(state_->metrics.completions[index]);
    result.winning_attempts_total[index] = load(state_->metrics.winners[index]);
  }
  if (result.logical_requests_total != 0) {
    const double attempts = static_cast<double>(result.backend_attempts_total[0]) +
                            static_cast<double>(result.backend_attempts_total[1]) +
                            static_cast<double>(result.backend_attempts_total[2]);
    result.attempt_amplification = attempts /
                                   static_cast<double>(result.logical_requests_total);
  }
  result.cancelled_attempts_total = load(state_->metrics.cancelled_attempts);
  for (std::size_t index = 0;
       index < result.cancellations_by_reason_total.size(); ++index) {
    result.cancellations_by_reason_total[index] =
        load(state_->metrics.cancellations_by_reason[index]);
  }
  result.wasted_attempt_time_us = load(state_->metrics.wasted_attempt_time_us);
  result.censored_attempts_total = load(state_->metrics.censored_attempts);
  result.hedge_started_total = load(state_->metrics.hedge_started);
  result.hedge_budget_denied_total = load(state_->metrics.hedge_budget_denied);
  result.hedge_deadline_denied_total = load(state_->metrics.hedge_deadline_denied);
  result.hedge_overload_denied_total = load(state_->metrics.hedge_overload_denied);
  result.hedge_no_target_total = load(state_->metrics.hedge_no_target);
  result.hedge_attempt_limit_total = load(state_->metrics.hedge_attempt_limit);
  result.hedge_dispatch_errors_total = load(state_->metrics.hedge_dispatch_errors);
  result.retry_started_total = load(state_->metrics.retry_started);
  result.retry_budget_denied_total = load(state_->metrics.retry_budget_denied);
  result.retry_deadline_denied_total = load(state_->metrics.retry_deadline_denied);
  result.retry_not_retryable_total = load(state_->metrics.retry_not_retryable);
  result.retry_no_target_total = load(state_->metrics.retry_no_target);
  result.retry_attempt_limit_total = load(state_->metrics.retry_attempt_limit);
  result.retry_same_replica_total = load(state_->metrics.retry_same_replica);
  result.pending_hedge_timers = load(state_->metrics.pending_hedge_timers);
  result.pending_retry_timers = load(state_->metrics.pending_retry_timers);
  result.active_attempts = load(state_->metrics.active_attempts);
  result.hedge_budget = state_->hedge_budget.snapshot();
  result.retry_budget = state_->retry_budget.snapshot();
  result.pending_backend_callbacks = state_->callback_drain->pending();
  return result;
}

std::shared_ptr<const control::ControllerSnapshot>
RouterService::controller_snapshot() const noexcept {
  return state_ && state_->controller ? state_->controller->snapshot() : nullptr;
}

bool RouterService::wait_for_attempt_callbacks(
    std::chrono::steady_clock::time_point deadline) const {
  return state_ && state_->wait_for_backend_callbacks(deadline);
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
  if (state_->dispatch_closed.load(std::memory_order_acquire) ||
      state_->shutdown_source.stop_requested()) {
    return reject(control::AdmissionResult::kCancelled,
                  grpc::StatusCode::CANCELLED, "router is shutting down");
  }
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
  request_context.method = "/artc.v1.Traffic/Execute";
  const MethodPolicy method_policy =
      state_->method_policy("/artc.v1.Traffic/Execute");

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
    LogicalRequest logical{std::move(request_context), method_policy, *request};
    auto state = std::make_shared<AttemptManager>(
        reactor, context, response, state_, std::move(logical), std::move(lease),
        std::move(permit), backend_index, std::move(decision_metadata));
    reactor->bind(state);
    state->start();
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

int wait_for_shutdown(grpc::Server& server, RouterService* router) {
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  std::jthread shutdown_thread([&server, router, signals](std::stop_token stop) {
    while (!stop.stop_requested()) {
      timespec timeout{0, 100'000'000};
      const int signal_number = sigtimedwait(&signals, nullptr, &timeout);
      if (signal_number == SIGINT || signal_number == SIGTERM) {
        if (router != nullptr) router->begin_shutdown();
        server.Shutdown(std::chrono::system_clock::now() + 5s);
        return;
      }
    }
  });
  server.Wait();
  shutdown_thread.request_stop();
  shutdown_thread.join();
  return router == nullptr || router->wait_for_attempt_callbacks(
                                  std::chrono::steady_clock::now() + 5s)
             ? 0
             : 1;
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
