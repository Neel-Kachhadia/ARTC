#pragma once

#include "artc/routing/selector.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace artc::control {

using SteadyClock = std::chrono::steady_clock;
using SteadyTime = SteadyClock::time_point;
using SystemTime = std::chrono::system_clock::time_point;

enum class AdmissionResult {
  kAdmitted,
  kDeadlineExpired,
  kDeadlineInfeasible,
  kConcurrencyLimit,
  kNoHealthyReplica,
  kShutdown,
  kInvalidRequest,
  kCancelled,
  kCount,
};

enum class RequestOutcome { kSuccess, kFailure, kTimeout, kDeadlineMiss, kCancelled };

[[nodiscard]] const char* admission_result_name(AdmissionResult result) noexcept;

struct RequestContext {
  std::uint64_t request_id{0};
  std::string_view method{"artc.v1.Traffic/Execute"};
  SteadyTime arrival_time{};
  SteadyTime effective_deadline{};
  SystemTime caller_deadline{SystemTime::max()};
  AdmissionResult admission_result{AdmissionResult::kDeadlineExpired};
  std::optional<RequestOutcome> terminal_outcome;
  std::optional<std::size_t> selected_replica;
  std::uint64_t controller_version{0};

  [[nodiscard]] std::chrono::nanoseconds remaining_budget(SteadyTime now) const noexcept;
  [[nodiscard]] SystemTime downstream_deadline(SteadyTime steady_now,
                                               SystemTime system_now) const noexcept;
};

[[nodiscard]] RequestContext make_request_context(
    std::uint64_t request_id, SystemTime caller_deadline, SteadyTime steady_now,
    SystemTime system_now, std::chrono::nanoseconds default_deadline);

struct AimdConfig {
  std::uint32_t min_limit{1};
  std::uint32_t max_limit{512};
  std::uint32_t initial_limit{64};
  std::uint32_t additive_increase{2};
  double multiplicative_decrease{0.7};
  std::chrono::milliseconds control_interval{100};
  std::uint64_t minimum_window_samples{16};
  std::chrono::microseconds target_latency{50'000};
  double overload_error_fraction{0.1};
};

struct HealthConfig {
  std::uint64_t minimum_latency_samples{4};
  std::uint64_t consecutive_failures_to_unavailable{3};
  std::uint64_t recovery_successes{3};
  std::chrono::milliseconds recovery_cooldown{500};
  double degraded_latency_ratio{2.0};
  double recovered_latency_ratio{1.5};
};

struct ControllerConfig {
  bool adaptive_selector{true};
  bool adaptive_concurrency{true};
  bool deadline_feasibility{true};
  AimdConfig aimd;
  HealthConfig health;
  double latency_ewma_smoothing{0.2};
  std::chrono::microseconds deadline_safety_margin{1'000};
  std::chrono::milliseconds default_deadline{5'000};
  double cold_start_latency_us{1'000.0};
  std::uint32_t recovery_probe_period{16};
  std::uint32_t decision_sample_every{0};
};

void validate(const ControllerConfig& config);

struct ReplicaSnapshot {
  std::string id;
  routing::HealthState health{routing::HealthState::kHealthy};
  std::uint64_t inflight{0};
  double latency_ewma_us{0.0};
  double latency_p95_us{0.0};
  double error_ewma{0.0};
  std::uint64_t latency_samples{0};
  std::uint64_t routed_total{0};
  std::uint64_t completed_total{0};
  std::uint64_t failed_total{0};
};

struct ControllerSnapshot {
  std::uint64_t version{1};
  SteadyTime published_at{};
  std::uint32_t route_limit{0};
  std::uint64_t route_inflight{0};
  std::uint64_t permits_acquired{0};
  std::uint64_t permits_released{0};
  std::uint64_t backend_attempts{0};
  std::uint64_t deadline_goodput{0};
  std::uint64_t deadline_misses{0};
  std::uint64_t deadline_infeasible{0};
  std::uint64_t controller_limit_changes{0};
  std::uint64_t controller_overload_events{0};
  std::uint64_t observation_drops{0};
  std::array<std::uint64_t, static_cast<std::size_t>(AdmissionResult::kCount)>
      admission_counts{};
  std::vector<ReplicaSnapshot> replicas;
};

struct GateSnapshot {
  std::uint32_t limit{0};
  std::uint64_t inflight{0};
  std::uint64_t acquired{0};
  std::uint64_t released{0};
  bool accepting{true};
};

class AdmissionGate;

class AdmissionPermit {
 public:
  AdmissionPermit() = default;
  ~AdmissionPermit();
  AdmissionPermit(AdmissionPermit&& other) noexcept;
  AdmissionPermit& operator=(AdmissionPermit&& other) noexcept;
  AdmissionPermit(const AdmissionPermit&) = delete;
  AdmissionPermit& operator=(const AdmissionPermit&) = delete;

  void release() noexcept;
  [[nodiscard]] bool owns_permit() const noexcept;

 private:
  struct State;
  friend class AdmissionGate;
  explicit AdmissionPermit(std::shared_ptr<State> state) noexcept;
  std::shared_ptr<State> state_;
};

struct AcquireResult {
  AdmissionResult result{AdmissionResult::kConcurrencyLimit};
  AdmissionPermit permit;
};

class AdmissionGate {
 public:
  AdmissionGate(std::uint32_t min_limit, std::uint32_t max_limit,
                std::uint32_t initial_limit);
  [[nodiscard]] AcquireResult try_acquire();
  void set_limit(std::uint32_t limit);
  void close() noexcept;
  [[nodiscard]] GateSnapshot snapshot() const noexcept;

 private:
  using State = AdmissionPermit::State;
  std::shared_ptr<State> state_;
};

struct FeasibilityResult {
  bool feasible{false};
  std::chrono::nanoseconds remaining{0};
  std::chrono::nanoseconds predicted_queue_delay{0};
  std::chrono::nanoseconds predicted_service_latency{0};
  std::chrono::nanoseconds safety_margin{0};
};

[[nodiscard]] FeasibilityResult evaluate_deadline_feasibility(
    const RequestContext& request, SteadyTime now,
    const ControllerSnapshot& snapshot,
    std::chrono::nanoseconds safety_margin,
    std::chrono::microseconds cold_start_latency);

struct CandidateScore {
  std::size_t replica_index{0};
  std::string_view replica_id;
  double latency_component{0.0};
  double load_component{0.0};
  double error_component{0.0};
  double health_component{0.0};
  double total{0.0};
  routing::HealthState health{routing::HealthState::kHealthy};
  std::uint64_t inflight{0};
};

struct SelectionResult {
  std::optional<std::size_t> selected;
  std::size_t eligible{0};
  CandidateScore selected_score;
};

class AdaptiveSelector {
 public:
  explicit AdaptiveSelector(const ControllerConfig& config);
  [[nodiscard]] SelectionResult select(
      const ControllerSnapshot& snapshot,
      std::span<const std::shared_ptr<routing::ReplicaState>> replicas,
      std::uint64_t request_id) noexcept;
  [[nodiscard]] std::vector<CandidateScore> explain(
      const ControllerSnapshot& snapshot,
      std::span<const std::shared_ptr<routing::ReplicaState>> replicas) const;

 private:
  [[nodiscard]] CandidateScore score(
      const ControllerSnapshot& snapshot,
      std::span<const std::shared_ptr<routing::ReplicaState>> replicas,
      std::size_t index, double best_latency) const noexcept;
  ControllerConfig config_;
  std::atomic<std::uint64_t> selection_sequence_{0};
};

class Phase2Controller {
 public:
  Phase2Controller(ControllerConfig config,
                   std::vector<std::shared_ptr<routing::ReplicaState>> replicas,
                   SteadyTime now = SteadyClock::now());
  ~Phase2Controller();
  Phase2Controller(const Phase2Controller&) = delete;
  Phase2Controller& operator=(const Phase2Controller&) = delete;

  [[nodiscard]] const ControllerConfig& config() const noexcept;
  [[nodiscard]] AcquireResult try_acquire();
  void record_admission(AdmissionResult result) noexcept;
  void record_backend_attempt(std::size_t replica_index) noexcept;
  bool record_completion(std::size_t replica_index, SteadyTime completed_at,
                         double latency_us, RequestOutcome outcome) noexcept;
  void record_pre_dispatch_deadline_miss() noexcept;
  void record_deadline_infeasible() noexcept;
  void tick(SteadyTime now, bool force = false);
  void start();
  void stop() noexcept;
  void close_admission() noexcept;
  [[nodiscard]] GateSnapshot admission_snapshot() const noexcept;
  [[nodiscard]] std::shared_ptr<const ControllerSnapshot> snapshot() const noexcept;

 private:
  struct HealthProgress {
    std::uint64_t slow_windows{0};
    std::uint64_t healthy_windows{0};
    std::uint64_t recovery_successes{0};
  };

  void run(std::stop_token stop_token);
  void update_health(SteadyTime now,
                     const std::vector<routing::ReplicaWindow>& windows,
                     const std::vector<routing::ReplicaStats>& stats);
  [[nodiscard]] std::shared_ptr<const ControllerSnapshot> make_snapshot(
      SteadyTime now, std::uint64_t version) const;
  void publish(SteadyTime now);

  ControllerConfig config_;
  std::vector<std::shared_ptr<routing::ReplicaState>> replicas_;
  AdmissionGate gate_;
  std::vector<HealthProgress> health_progress_;
  SteadyTime last_tick_;
  mutable std::mutex tick_mutex_;
  std::atomic<std::shared_ptr<const ControllerSnapshot>> published_;
  std::array<std::atomic<std::uint64_t>,
             static_cast<std::size_t>(AdmissionResult::kCount)> admission_counts_{};
  std::atomic<std::uint64_t> backend_attempts_{0};
  std::atomic<std::uint64_t> deadline_goodput_{0};
  std::atomic<std::uint64_t> deadline_misses_{0};
  std::atomic<std::uint64_t> deadline_infeasible_{0};
  std::atomic<std::uint64_t> controller_limit_changes_{0};
  std::atomic<std::uint64_t> controller_overload_events_{0};
  std::atomic<std::uint64_t> observation_drops_{0};
  mutable std::mutex worker_mutex_;
  mutable std::mutex worker_lifecycle_mutex_;
  std::condition_variable worker_cv_;
  bool worker_stopping_{false};
  std::jthread worker_;
};

}  // namespace artc::control
