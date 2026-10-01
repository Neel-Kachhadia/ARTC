#include "artc/control/phase2.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <utility>

namespace artc::control {
namespace {

template <typename T>
void increment_saturated(std::atomic<T>* value) noexcept {
  T current = value->load(std::memory_order_relaxed);
  while (current != std::numeric_limits<T>::max() &&
         !value->compare_exchange_weak(current, static_cast<T>(current + 1),
                                       std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {}
}

template <typename TimePoint, typename Duration>
TimePoint saturating_add(TimePoint point, Duration duration) noexcept {
  if (duration <= Duration::zero()) return point;
  using TargetDuration = typename TimePoint::duration;
  const long double point_ticks =
      std::chrono::duration<long double, typename TargetDuration::period>(
          point.time_since_epoch()).count();
  const long double maximum_ticks =
      std::chrono::duration<long double, typename TargetDuration::period>(
          TimePoint::max().time_since_epoch()).count();
  const long double addition_ticks = std::trunc(
      std::chrono::duration<long double, typename TargetDuration::period>(duration).count());
  if (addition_ticks <= 0.0L) return point;
  const long double result_ticks = point_ticks + addition_ticks;
  if (result_ticks >= maximum_ticks) return TimePoint::max();
  return TimePoint(TargetDuration(static_cast<typename TargetDuration::rep>(result_ticks)));
}

std::chrono::nanoseconds saturating_nanoseconds(long double value) noexcept {
  if (value <= 0.0L) return std::chrono::nanoseconds::zero();
  if (value >= static_cast<long double>(std::chrono::nanoseconds::max().count())) {
    return std::chrono::nanoseconds::max();
  }
  return std::chrono::nanoseconds(static_cast<std::int64_t>(value));
}

std::chrono::nanoseconds saturating_sum(std::chrono::nanoseconds a,
                                       std::chrono::nanoseconds b) noexcept {
  if (b.count() > 0 && a.count() > std::chrono::nanoseconds::max().count() - b.count()) {
    return std::chrono::nanoseconds::max();
  }
  return a + b;
}

std::size_t admission_index(AdmissionResult result) noexcept {
  return static_cast<std::size_t>(result);
}

double selector_latency_estimate(const ReplicaSnapshot& replica,
                                 double cold_start_us,
                                 std::uint64_t minimum_samples) noexcept {
  const double completed = replica.latency_samples == 0
                               ? cold_start_us
                               : std::max(replica.latency_ewma_us, 1.0);
  const double censored = replica.censored_latency_samples >= minimum_samples
                              ? replica.censored_latency_p95_lower_bound_us
                              : 0.0;
  return std::max(completed, censored);
}

}  // namespace

const char* admission_result_name(AdmissionResult result) noexcept {
  switch (result) {
    case AdmissionResult::kAdmitted: return "ADMITTED";
    case AdmissionResult::kDeadlineExpired: return "REJECT_DEADLINE_EXPIRED";
    case AdmissionResult::kDeadlineInfeasible: return "REJECT_DEADLINE_INFEASIBLE";
    case AdmissionResult::kConcurrencyLimit: return "REJECT_CONCURRENCY_LIMIT";
    case AdmissionResult::kNoHealthyReplica: return "REJECT_NO_HEALTHY_REPLICA";
    case AdmissionResult::kShutdown: return "REJECT_SHUTDOWN";
    case AdmissionResult::kInvalidRequest: return "REJECT_INVALID_REQUEST";
    case AdmissionResult::kCancelled: return "REJECT_CANCELLED";
    case AdmissionResult::kCount: break;
  }
  return "REJECT_INVALID";
}

std::chrono::nanoseconds RequestContext::remaining_budget(SteadyTime now) const noexcept {
  if (now >= effective_deadline) return std::chrono::nanoseconds::zero();
  const long double deadline_ns = std::chrono::duration<long double, std::nano>(
      effective_deadline.time_since_epoch()).count();
  const long double now_ns = std::chrono::duration<long double, std::nano>(
      now.time_since_epoch()).count();
  return saturating_nanoseconds(deadline_ns - now_ns);
}

SystemTime RequestContext::downstream_deadline(SteadyTime steady_now,
                                               SystemTime system_now) const noexcept {
  const auto remaining = remaining_budget(steady_now);
  const auto from_remaining = saturating_add(system_now, remaining);
  return std::min(caller_deadline, from_remaining);
}

RequestContext make_request_context(std::uint64_t request_id,
                                    SystemTime caller_deadline,
                                    SteadyTime steady_now,
                                    SystemTime system_now,
                                    std::chrono::nanoseconds default_deadline) {
  if (default_deadline <= std::chrono::nanoseconds::zero()) {
    throw std::invalid_argument("default deadline must be positive");
  }
  std::chrono::nanoseconds budget = default_deadline;
  if (caller_deadline != SystemTime::max()) {
    if (caller_deadline <= system_now) {
      budget = std::chrono::nanoseconds::zero();
    } else {
      const long double deadline_ns =
          std::chrono::duration<long double, std::nano>(
              caller_deadline.time_since_epoch()).count();
      const long double now_ns = std::chrono::duration<long double, std::nano>(
                                     system_now.time_since_epoch()).count();
      const long double nanos = deadline_ns - now_ns;
      if (nanos >= static_cast<long double>(std::chrono::nanoseconds::max().count())) {
        budget = std::chrono::nanoseconds::max();
      } else {
        budget = std::chrono::nanoseconds(static_cast<std::int64_t>(nanos));
      }
    }
  }
  RequestContext request;
  request.request_id = request_id;
  request.arrival_time = steady_now;
  request.effective_deadline = saturating_add(steady_now, budget);
  request.caller_deadline = caller_deadline;
  return request;
}

void validate(const ControllerConfig& config) {
  const auto& aimd = config.aimd;
  const auto& health = config.health;
  if (aimd.min_limit == 0 || aimd.max_limit < aimd.min_limit ||
      aimd.max_limit > kHardMaxRouteConcurrency ||
      aimd.initial_limit < aimd.min_limit || aimd.initial_limit > aimd.max_limit) {
    throw std::invalid_argument("invalid or excessive AIMD concurrency limits");
  }
  if (aimd.additive_increase == 0 || !std::isfinite(aimd.multiplicative_decrease) ||
      aimd.multiplicative_decrease <= 0.0 || aimd.multiplicative_decrease >= 1.0) {
    throw std::invalid_argument("invalid AIMD increase or decrease factor");
  }
  if (aimd.control_interval <= std::chrono::milliseconds::zero() ||
      aimd.control_interval > std::chrono::hours(24) ||
      aimd.minimum_window_samples == 0 || aimd.target_latency <= std::chrono::microseconds::zero() ||
      !std::isfinite(aimd.overload_error_fraction) || aimd.overload_error_fraction <= 0.0 ||
      aimd.overload_error_fraction > 1.0) {
    throw std::invalid_argument("invalid AIMD control window");
  }
  if (health.minimum_latency_samples == 0 ||
      health.consecutive_failures_to_unavailable < 2 || health.recovery_successes == 0 ||
      health.recovery_cooldown <= std::chrono::milliseconds::zero() ||
      !std::isfinite(health.degraded_latency_ratio) || health.degraded_latency_ratio <= 1.0 ||
      !std::isfinite(health.recovered_latency_ratio) ||
      health.recovered_latency_ratio <= 1.0 ||
      health.recovered_latency_ratio >= health.degraded_latency_ratio) {
    throw std::invalid_argument("invalid replica health thresholds");
  }
  if (!std::isfinite(config.latency_ewma_smoothing) ||
      config.latency_ewma_smoothing <= 0.0 || config.latency_ewma_smoothing > 1.0 ||
      config.deadline_safety_margin < std::chrono::microseconds::zero() ||
      config.deadline_safety_margin > std::chrono::hours(24) ||
      config.default_deadline <= std::chrono::milliseconds::zero() ||
      config.default_deadline > std::chrono::hours(24) ||
      !std::isfinite(config.cold_start_latency_us) || config.cold_start_latency_us <= 0.0 ||
      config.cold_start_latency_us > 60'000'000.0 ||
      config.recovery_probe_period == 0) {
    throw std::invalid_argument("invalid Phase 2 controller configuration");
  }
}

struct AdmissionPermit::State {
  State(std::uint32_t minimum, std::uint32_t maximum, std::uint32_t initial)
      : min_limit(minimum), max_limit(maximum), limit(initial) {}
  mutable std::mutex mutex;
  std::uint32_t min_limit;
  std::uint32_t max_limit;
  std::uint32_t limit;
  std::uint64_t inflight{0};
  std::uint64_t acquired{0};
  std::uint64_t released{0};
  bool accepting{true};
};

AdmissionPermit::AdmissionPermit(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

AdmissionPermit::~AdmissionPermit() { release(); }

AdmissionPermit::AdmissionPermit(AdmissionPermit&& other) noexcept
    : state_(std::move(other.state_)) {}

AdmissionPermit& AdmissionPermit::operator=(AdmissionPermit&& other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
  }
  return *this;
}

void AdmissionPermit::release() noexcept {
  if (!state_) return;
  const auto state = std::move(state_);
  std::lock_guard lock(state->mutex);
  if (state->inflight == 0 || state->released >= state->acquired) std::terminate();
  --state->inflight;
  ++state->released;
}

bool AdmissionPermit::owns_permit() const noexcept { return static_cast<bool>(state_); }

AdmissionGate::AdmissionGate(std::uint32_t min_limit, std::uint32_t max_limit,
                             std::uint32_t initial_limit)
    : state_(std::make_shared<State>(min_limit, max_limit, initial_limit)) {
  if (min_limit == 0 || max_limit < min_limit || initial_limit < min_limit ||
      initial_limit > max_limit) {
    throw std::invalid_argument("invalid admission gate limits");
  }
}

AcquireResult AdmissionGate::try_acquire() {
  std::lock_guard lock(state_->mutex);
  if (!state_->accepting) return {AdmissionResult::kShutdown, {}};
  if (state_->inflight >= state_->limit ||
      state_->inflight == std::numeric_limits<std::uint64_t>::max() ||
      state_->acquired == std::numeric_limits<std::uint64_t>::max()) {
    return {AdmissionResult::kConcurrencyLimit, {}};
  }
  ++state_->inflight;
  ++state_->acquired;
  return {AdmissionResult::kAdmitted, AdmissionPermit(state_)};
}

void AdmissionGate::set_limit(std::uint32_t limit) {
  std::lock_guard lock(state_->mutex);
  if (limit < state_->min_limit || limit > state_->max_limit) {
    throw std::out_of_range("admission limit outside configured bounds");
  }
  state_->limit = limit;
}

void AdmissionGate::close() noexcept {
  std::lock_guard lock(state_->mutex);
  state_->accepting = false;
}

GateSnapshot AdmissionGate::snapshot() const noexcept {
  std::lock_guard lock(state_->mutex);
  return GateSnapshot{state_->limit, state_->inflight, state_->acquired,
                      state_->released, state_->accepting};
}

FeasibilityResult evaluate_deadline_feasibility(
    const RequestContext& request, SteadyTime now,
    const ControllerSnapshot& snapshot, std::chrono::nanoseconds safety_margin,
    std::chrono::microseconds cold_start_latency) {
  FeasibilityResult result;
  result.remaining = request.remaining_budget(now);
  result.safety_margin = std::max(safety_margin, std::chrono::nanoseconds::zero());
  if (result.remaining == std::chrono::nanoseconds::zero()) return result;

  double fastest_us = std::numeric_limits<double>::infinity();
  double second_fastest_us = std::numeric_limits<double>::infinity();
  for (const auto& replica : snapshot.replicas) {
    if (replica.health == routing::HealthState::kUnavailable) continue;
    // Censored hedge losers are biased lower bounds, not service percentiles;
    // keep Phase 2 deadline feasibility based on completed-attempt evidence.
    const double estimate = replica.latency_p95_us > 0.0
                                ? replica.latency_p95_us
                                : replica.latency_ewma_us;
    if (!std::isfinite(estimate) || estimate <= 0.0) continue;
    if (estimate < fastest_us) {
      second_fastest_us = fastest_us;
      fastest_us = estimate;
    } else if (estimate < second_fastest_us) {
      second_fastest_us = estimate;
    }
  }
  // One straggler cannot poison feasibility; two estimates prevent one
  // optimistic outlier from admitting traffic on multiple available replicas.
  const double service_us = std::isfinite(second_fastest_us)
                                ? second_fastest_us
                                : fastest_us;
  const double service_estimate_us =
      std::isfinite(service_us) && service_us > 0.0
          ? service_us
          : std::chrono::duration<double, std::micro>(cold_start_latency).count();
  const long double service_ns = static_cast<long double>(service_estimate_us) * 1'000.0L;
  result.predicted_service_latency = std::chrono::nanoseconds(
      service_ns >= static_cast<long double>(std::chrono::nanoseconds::max().count())
          ? std::chrono::nanoseconds::max().count()
          : static_cast<std::int64_t>(std::ceil(service_ns)));

  // ponytail: fail-fast admission creates no queue; model queue delay as zero until a bounded queue exists.
  result.predicted_queue_delay = std::chrono::nanoseconds::zero();

  auto estimated = saturating_sum(result.predicted_queue_delay,
                                  result.predicted_service_latency);
  estimated = saturating_sum(estimated, result.safety_margin);
  result.feasible = estimated < result.remaining;
  return result;
}

AdaptiveSelector::AdaptiveSelector(const ControllerConfig& config) : config_(config) {
  validate(config_);
}

CandidateScore AdaptiveSelector::score(
    const ControllerSnapshot& snapshot,
    std::span<const std::shared_ptr<routing::ReplicaState>> replicas,
    std::size_t index, double best_latency) const noexcept {
  CandidateScore result;
  result.replica_index = index;
  if (index >= replicas.size() || index >= snapshot.replicas.size() || !replicas[index]) {
    result.total = std::numeric_limits<double>::infinity();
    return result;
  }
  const auto& view = snapshot.replicas[index];
  result.replica_id = replicas[index]->id;
  result.health = view.health;
  result.inflight = replicas[index]->inflight.load(std::memory_order_relaxed);
  const double latency = selector_latency_estimate(
      view, config_.cold_start_latency_us, config_.health.minimum_latency_samples);
  result.latency_component = std::clamp(latency / std::max(best_latency, 1.0), 1.0, 16.0);
  result.load_component = 1.0 + std::min(
      static_cast<double>(result.inflight) /
          static_cast<double>(std::max<std::uint32_t>(1, snapshot.route_limit)),
      16.0);
  result.error_component = 1.0 + 4.0 * std::clamp(view.error_ewma, 0.0, 1.0);
  switch (view.health) {
    case routing::HealthState::kHealthy: result.health_component = 1.0; break;
    case routing::HealthState::kDegraded: result.health_component = 2.0; break;
    case routing::HealthState::kRecovering: result.health_component = 4.0; break;
    case routing::HealthState::kUnavailable:
      result.health_component = std::numeric_limits<double>::infinity();
      break;
  }
  result.total = result.latency_component * result.load_component *
                 result.error_component * result.health_component;
  if (!std::isfinite(result.total)) result.total = std::numeric_limits<double>::infinity();
  return result;
}

SelectionResult AdaptiveSelector::select(
    const ControllerSnapshot& snapshot,
    std::span<const std::shared_ptr<routing::ReplicaState>> replicas,
    std::uint64_t request_id,
    std::optional<std::size_t> excluded_index) noexcept {
  SelectionResult result;
  if (replicas.empty() || replicas.size() != snapshot.replicas.size()) return result;
  if (excluded_index && *excluded_index >= replicas.size()) return result;
  const auto excluded = [excluded_index](std::size_t index) noexcept {
    return excluded_index && *excluded_index == index;
  };
  double best_latency = std::numeric_limits<double>::infinity();
  std::size_t cold_count = 0;
  std::size_t healthy_count = 0;
  std::size_t degraded_count = 0;
  std::size_t recovering_count = 0;
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    if (excluded(i) || !replicas[i] ||
        snapshot.replicas[i].health == routing::HealthState::kUnavailable) continue;
    ++result.eligible;
    const auto health = snapshot.replicas[i].health;
    if (health == routing::HealthState::kRecovering) {
      ++recovering_count;
      continue;
    }
    if (health == routing::HealthState::kHealthy) ++healthy_count;
    if (health == routing::HealthState::kDegraded) ++degraded_count;
    if (snapshot.replicas[i].latency_samples < config_.health.minimum_latency_samples &&
        snapshot.replicas[i].censored_latency_samples <
            config_.health.minimum_latency_samples) {
      ++cold_count;
    } else {
      best_latency = std::min(best_latency, selector_latency_estimate(
          snapshot.replicas[i], config_.cold_start_latency_us,
          config_.health.minimum_latency_samples));
    }
  }
  if (result.eligible == 0) return result;
  if (!std::isfinite(best_latency)) best_latency = config_.cold_start_latency_us;

  const auto sequence = selection_sequence_.fetch_add(1, std::memory_order_relaxed);
  if (sequence != 0 && sequence % config_.recovery_probe_period == 0) {
    const auto select_probe = [&](routing::HealthState health,
                                  std::size_t count) noexcept {
      auto desired = static_cast<std::size_t>(
          (sequence / config_.recovery_probe_period) % count);
      for (std::size_t i = 0; i < replicas.size(); ++i) {
        if (excluded(i) || !replicas[i] || snapshot.replicas[i].health != health) continue;
        if (desired-- == 0) {
          result.selected = i;
          result.selected_score = score(snapshot, replicas, i, best_latency);
          return true;
        }
      }
      return false;
    };
    if (recovering_count != 0 &&
        select_probe(routing::HealthState::kRecovering, recovering_count)) return result;
    if (degraded_count != 0 &&
        select_probe(routing::HealthState::kDegraded, degraded_count)) return result;
    if (cold_count == 0 && healthy_count != 0 &&
        select_probe(routing::HealthState::kHealthy, healthy_count)) return result;
  }

  if (cold_count != 0) {
    const auto start = static_cast<std::size_t>(request_id % replicas.size());
    for (std::size_t offset = 0; offset < replicas.size(); ++offset) {
      const auto index = (start + offset) % replicas.size();
      if (!excluded(index) && replicas[index] &&
          snapshot.replicas[index].health != routing::HealthState::kRecovering &&
          snapshot.replicas[index].health != routing::HealthState::kUnavailable &&
          snapshot.replicas[index].latency_samples < config_.health.minimum_latency_samples &&
          snapshot.replicas[index].censored_latency_samples <
              config_.health.minimum_latency_samples) {
        result.selected = index;
        result.selected_score = score(snapshot, replicas, index, best_latency);
        return result;
      }
    }
  }

  double best_score = std::numeric_limits<double>::infinity();
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    if (excluded(i) || !replicas[i] ||
        snapshot.replicas[i].health == routing::HealthState::kUnavailable ||
        snapshot.replicas[i].health == routing::HealthState::kRecovering) continue;
    const auto candidate = score(snapshot, replicas, i, best_latency);
    if (candidate.total < best_score) {
      best_score = candidate.total;
      result.selected = i;
      result.selected_score = candidate;
    }
  }
  return result;
}

SelectionResult AdaptiveSelector::select_secondary(
    const ControllerSnapshot& snapshot,
    std::span<const std::shared_ptr<routing::ReplicaState>> replicas,
    std::span<const std::size_t> excluded_indices) const noexcept {
  SelectionResult result;
  if (replicas.empty() || replicas.size() != snapshot.replicas.size()) return result;
  const auto excluded = [excluded_indices](std::size_t index) noexcept {
    return std::find(excluded_indices.begin(), excluded_indices.end(), index) !=
           excluded_indices.end();
  };
  double best_latency = std::numeric_limits<double>::infinity();
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    if (!replicas[i] || excluded(i) ||
        snapshot.replicas[i].health != routing::HealthState::kHealthy) continue;
    ++result.eligible;
    if (snapshot.replicas[i].latency_samples >= config_.health.minimum_latency_samples ||
        snapshot.replicas[i].censored_latency_samples >=
            config_.health.minimum_latency_samples) {
      best_latency = std::min(best_latency, selector_latency_estimate(
          snapshot.replicas[i], config_.cold_start_latency_us,
          config_.health.minimum_latency_samples));
    }
  }
  if (result.eligible == 0) return result;
  if (!std::isfinite(best_latency)) best_latency = config_.cold_start_latency_us;

  double best_score = std::numeric_limits<double>::infinity();
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    if (!replicas[i] || excluded(i) ||
        snapshot.replicas[i].health != routing::HealthState::kHealthy) continue;
    const auto candidate = score(snapshot, replicas, i, best_latency);
    if (candidate.total < best_score) {
      best_score = candidate.total;
      result.selected = i;
      result.selected_score = candidate;
    }
  }
  return result;
}

std::vector<CandidateScore> AdaptiveSelector::explain(
    const ControllerSnapshot& snapshot,
    std::span<const std::shared_ptr<routing::ReplicaState>> replicas) const {
  std::vector<CandidateScore> result;
  if (replicas.size() != snapshot.replicas.size()) return result;
  double best_latency = std::numeric_limits<double>::infinity();
  for (const auto& replica : snapshot.replicas) {
    if (replica.health != routing::HealthState::kUnavailable &&
        (replica.latency_samples >= config_.health.minimum_latency_samples ||
         replica.censored_latency_samples >= config_.health.minimum_latency_samples)) {
      best_latency = std::min(best_latency, selector_latency_estimate(
          replica, config_.cold_start_latency_us,
          config_.health.minimum_latency_samples));
    }
  }
  if (!std::isfinite(best_latency)) best_latency = config_.cold_start_latency_us;
  result.reserve(replicas.size());
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    result.push_back(score(snapshot, replicas, i, best_latency));
  }
  return result;
}

Phase2Controller::Phase2Controller(
    ControllerConfig config,
    std::vector<std::shared_ptr<routing::ReplicaState>> replicas,
    SteadyTime now)
    : config_(std::move(config)),
      replicas_(std::move(replicas)),
      gate_(config_.aimd.min_limit, config_.aimd.max_limit,
            config_.adaptive_concurrency ? config_.aimd.initial_limit : config_.aimd.max_limit),
      health_progress_(replicas_.size()),
      last_tick_(now) {
  validate(config_);
  if (replicas_.empty() || replicas_.size() > 64) {
    throw std::invalid_argument("controller requires 1 to 64 replicas");
  }
  for (std::size_t i = 0; i < replicas_.size(); ++i) {
    if (!replicas_[i]) throw std::invalid_argument("controller replica cannot be null");
    for (std::size_t j = 0; j < i; ++j) {
      if (replicas_[i]->id == replicas_[j]->id || replicas_[i]->address == replicas_[j]->address) {
        throw std::invalid_argument("controller replica identities and addresses must be unique");
      }
    }
  }
  published_.store(make_snapshot(now, 1), std::memory_order_release);
}

Phase2Controller::~Phase2Controller() { stop(); }

const ControllerConfig& Phase2Controller::config() const noexcept { return config_; }

AcquireResult Phase2Controller::try_acquire() { return gate_.try_acquire(); }

void Phase2Controller::record_admission(AdmissionResult result) noexcept {
  const auto index = admission_index(result);
  if (index >= admission_counts_.size()) return;
  increment_saturated(&admission_counts_[index]);
}

void Phase2Controller::record_backend_attempt(std::size_t replica_index) noexcept {
  if (replica_index >= replicas_.size()) {
    increment_saturated(&observation_drops_);
    return;
  }
  replicas_[replica_index]->record_routed();
  increment_saturated(&backend_attempts_);
}

bool Phase2Controller::record_completion(std::size_t replica_index,
                                         SteadyTime completed_at,
                                         double latency_us,
                                         RequestOutcome outcome) noexcept {
  if (!record_attempt_completion(replica_index, completed_at, latency_us, outcome)) {
    return false;
  }
  record_logical_completion(outcome);
  return true;
}

bool Phase2Controller::record_attempt_completion(std::size_t replica_index,
                                                 SteadyTime completed_at,
                                                 double latency_us,
                                                 RequestOutcome outcome) noexcept {
  if (replica_index >= replicas_.size()) {
    increment_saturated(&observation_drops_);
    return false;
  }
  const bool success = outcome == RequestOutcome::kSuccess ||
                       outcome == RequestOutcome::kDeadlineMiss;
  const bool timeout = outcome == RequestOutcome::kTimeout ||
                       outcome == RequestOutcome::kDeadlineMiss;
  const bool missed = outcome == RequestOutcome::kTimeout ||
                      outcome == RequestOutcome::kDeadlineMiss;
  const bool cancelled = outcome == RequestOutcome::kCancelled;
  if (!replicas_[replica_index]->observe_completion(
          completed_at, latency_us, success, timeout, missed, cancelled,
          config_.latency_ewma_smoothing)) {
    increment_saturated(&observation_drops_);
    return false;
  }
  return true;
}

void Phase2Controller::record_logical_completion(RequestOutcome outcome) noexcept {
  if (outcome == RequestOutcome::kSuccess) increment_saturated(&deadline_goodput_);
  if (outcome == RequestOutcome::kTimeout || outcome == RequestOutcome::kDeadlineMiss) {
    increment_saturated(&deadline_misses_);
  }
}

void Phase2Controller::record_pre_dispatch_deadline_miss() noexcept {
  increment_saturated(&deadline_misses_);
}

void Phase2Controller::record_deadline_infeasible() noexcept {
  increment_saturated(&deadline_infeasible_);
}

void Phase2Controller::tick(SteadyTime now, bool force) {
  std::lock_guard tick_lock(tick_mutex_);
  if (now < last_tick_ || (!force && now - last_tick_ < config_.aimd.control_interval)) return;

  std::vector<routing::ReplicaWindow> windows;
  std::vector<routing::ReplicaStats> stats;
  windows.reserve(replicas_.size());
  stats.reserve(replicas_.size());
  std::uint64_t samples = 0;
  std::uint64_t failures = 0;
  for (const auto& replica : replicas_) {
    windows.push_back(replica->take_window());
    stats.push_back(replica->stats(now));
    samples += windows.back().control_samples;
    failures += windows.back().failed;
  }

  update_health(now, windows, stats);
  stats.clear();
  for (const auto& replica : replicas_) stats.push_back(replica->stats(now));

  if (config_.adaptive_concurrency && samples >= config_.aimd.minimum_window_samples) {
    const auto current = gate_.snapshot().limit;
    const auto error_fraction = static_cast<double>(failures) / static_cast<double>(samples);
    double max_latency = 0.0;
    bool healthy_replica_observed = false;
    for (const auto& replica : stats) {
      if (replica.health == routing::HealthState::kHealthy) {
        healthy_replica_observed = true;
        max_latency = std::max(max_latency, replica.latency_ewma_us);
      }
    }
    if (!healthy_replica_observed) {
      for (const auto& replica : stats) {
        if (replica.health != routing::HealthState::kUnavailable) {
          max_latency = std::max(max_latency, replica.latency_ewma_us);
        }
      }
    }
    const bool overload = error_fraction >= config_.aimd.overload_error_fraction ||
        max_latency > static_cast<double>(config_.aimd.target_latency.count());
    std::uint32_t next = current;
    if (overload) {
      const double reduced = std::floor(static_cast<double>(current) *
                                        config_.aimd.multiplicative_decrease);
      next = std::max(config_.aimd.min_limit,
                      static_cast<std::uint32_t>(std::max(0.0, reduced)));
      increment_saturated(&controller_overload_events_);
    } else {
      const auto room = config_.aimd.max_limit - current;
      next = current + std::min(config_.aimd.additive_increase, room);
    }
    if (next != current) {
      gate_.set_limit(next);
      increment_saturated(&controller_limit_changes_);
    }
  }

  last_tick_ = now;
  publish(now);
}

void Phase2Controller::update_health(SteadyTime now,
    const std::vector<routing::ReplicaWindow>& windows,
    const std::vector<routing::ReplicaStats>& stats) {
  double baseline = std::numeric_limits<double>::infinity();
  for (std::size_t i = 0; i < stats.size(); ++i) {
    if (stats[i].health != routing::HealthState::kUnavailable &&
        stats[i].latency_samples >= config_.health.minimum_latency_samples &&
        windows[i].succeeded != 0 &&
        windows[i].latency_p95_us > 0.0) {
      baseline = std::min(baseline, windows[i].latency_p95_us);
    }
  }

  for (std::size_t i = 0; i < replicas_.size(); ++i) {
    auto health = stats[i].health;
    auto& progress = health_progress_[i];
    if (health != routing::HealthState::kUnavailable &&
        stats[i].consecutive_failures >= config_.health.consecutive_failures_to_unavailable) {
      health = routing::HealthState::kUnavailable;
      progress = {};
    } else if (health == routing::HealthState::kUnavailable) {
      if (stats[i].has_failure &&
          now >= saturating_add(stats[i].last_failure, config_.health.recovery_cooldown)) {
        health = routing::HealthState::kRecovering;
        progress = {};
      }
    } else if (health == routing::HealthState::kRecovering) {
      if (windows[i].failed != 0) {
        health = routing::HealthState::kUnavailable;
        progress.recovery_successes = 0;
      } else {
        const auto remaining = config_.health.recovery_successes -
            std::min(progress.recovery_successes, config_.health.recovery_successes);
        progress.recovery_successes +=
            std::min(remaining, windows[i].useful_successes);
        if (progress.recovery_successes >= config_.health.recovery_successes) {
          health = routing::HealthState::kHealthy;
          progress = {};
        }
      }
    } else if (std::isfinite(baseline) && baseline > 0.0 &&
               stats[i].latency_samples >= config_.health.minimum_latency_samples &&
               windows[i].succeeded != 0) {
      const double ratio = windows[i].latency_p95_us / baseline;
      if (ratio >= config_.health.degraded_latency_ratio) {
        progress.slow_windows = std::min<std::uint64_t>(2, progress.slow_windows + 1);
        progress.healthy_windows = 0;
        if (progress.slow_windows >= 2) health = routing::HealthState::kDegraded;
      } else if (ratio <= config_.health.recovered_latency_ratio) {
        progress.healthy_windows = std::min<std::uint64_t>(2, progress.healthy_windows + 1);
        progress.slow_windows = 0;
        if (progress.healthy_windows >= 2) health = routing::HealthState::kHealthy;
      }
    }
    if (health != stats[i].health) replicas_[i]->set_health(health);
  }
}

void Phase2Controller::start() {
  std::lock_guard lifecycle_lock(worker_lifecycle_mutex_);
  std::lock_guard lock(worker_mutex_);
  if (worker_.joinable()) return;
  worker_stopping_ = false;
  worker_ = std::jthread([this](std::stop_token stop_token) { run(stop_token); });
}

void Phase2Controller::stop() noexcept {
  std::lock_guard lifecycle_lock(worker_lifecycle_mutex_);
  {
    std::lock_guard lock(worker_mutex_);
    worker_stopping_ = true;
  }
  worker_cv_.notify_all();
  if (worker_.joinable()) {
    worker_.request_stop();
    if (worker_.get_id() != std::this_thread::get_id()) worker_.join();
  }
}

void Phase2Controller::close_admission() noexcept {
  gate_.close();
  stop();
}

GateSnapshot Phase2Controller::admission_snapshot() const noexcept {
  return gate_.snapshot();
}

std::shared_ptr<const ControllerSnapshot> Phase2Controller::snapshot() const noexcept {
  return published_.load(std::memory_order_acquire);
}

void Phase2Controller::run(std::stop_token stop_token) {
  std::unique_lock lock(worker_mutex_);
  while (!worker_stopping_ && !stop_token.stop_requested()) {
    worker_cv_.wait_for(lock, config_.aimd.control_interval,
                        [this, &stop_token] {
                          return worker_stopping_ || stop_token.stop_requested();
                        });
    if (worker_stopping_ || stop_token.stop_requested()) break;
    lock.unlock();
    tick(SteadyClock::now());
    lock.lock();
  }
}

std::shared_ptr<const ControllerSnapshot> Phase2Controller::make_snapshot(
    SteadyTime now, std::uint64_t version) const {
  auto result = std::make_shared<ControllerSnapshot>();
  const auto gate = gate_.snapshot();
  result->version = version;
  result->published_at = now;
  result->route_limit = gate.limit;
  result->route_inflight = gate.inflight;
  result->permits_acquired = gate.acquired;
  result->permits_released = gate.released;
  result->backend_attempts = backend_attempts_.load(std::memory_order_relaxed);
  result->deadline_goodput = deadline_goodput_.load(std::memory_order_relaxed);
  result->deadline_misses = deadline_misses_.load(std::memory_order_relaxed);
  result->deadline_infeasible = deadline_infeasible_.load(std::memory_order_relaxed);
  result->controller_limit_changes =
      controller_limit_changes_.load(std::memory_order_relaxed);
  result->controller_overload_events =
      controller_overload_events_.load(std::memory_order_relaxed);
  result->observation_drops = observation_drops_.load(std::memory_order_relaxed);
  for (std::size_t i = 0; i < admission_counts_.size(); ++i) {
    result->admission_counts[i] = admission_counts_[i].load(std::memory_order_relaxed);
  }
  result->replicas.reserve(replicas_.size());
  for (const auto& replica : replicas_) {
    const auto state = replica->stats(now);
    result->replicas.push_back(ReplicaSnapshot{
        replica->id, state.health, replica->inflight.load(std::memory_order_relaxed),
        state.latency_ewma_us, state.latency_p95_us, state.error_ewma,
        state.latency_samples, state.censored_latency_p95_lower_bound_us,
        state.censored_latency_samples, state.routed, state.completed, state.failed,
        state.timed_out});
  }
  return result;
}

void Phase2Controller::publish(SteadyTime now) {
  const auto current = snapshot();
  const auto version = current->version == std::numeric_limits<std::uint64_t>::max()
                           ? current->version
                           : current->version + 1;
  published_.store(make_snapshot(now, version), std::memory_order_release);
}

}  // namespace artc::control
