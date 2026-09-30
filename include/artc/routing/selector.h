#pragma once

#include <atomic>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace artc::routing {

enum class HealthState { kHealthy, kDegraded, kUnavailable, kRecovering };

struct ReplicaStats {
  double latency_ewma_us{0.0};
  double error_ewma{0.0};
  double latency_p95_us{0.0};
  std::uint64_t latency_samples{0};
  double censored_latency_p95_lower_bound_us{0.0};
  std::uint64_t censored_latency_samples{0};
  std::uint64_t completed{0};
  std::uint64_t succeeded{0};
  std::uint64_t failed{0};
  std::uint64_t timed_out{0};
  std::uint64_t routed{0};
  std::uint64_t consecutive_failures{0};
  std::chrono::steady_clock::time_point last_success{};
  std::chrono::steady_clock::time_point last_failure{};
  bool has_success{false};
  bool has_failure{false};
  HealthState health{HealthState::kHealthy};
};

struct ReplicaWindow {
  std::uint64_t completed{0};
  std::uint64_t control_samples{0};
  std::uint64_t succeeded{0};
  std::uint64_t useful_successes{0};
  std::uint64_t failed{0};
  std::uint64_t timed_out{0};
  std::uint64_t deadline_missed{0};
  double latency_p95_us{0.0};
};

struct ReplicaState {
  static constexpr std::size_t kLatencyWindowCapacity = 128;
  // Keep censored latency as a short-lived routing signal so old hedge losses
  // cannot permanently penalize a replica that has recovered.
  static constexpr auto kCensoredLatencyHorizon = std::chrono::seconds(5);

  ReplicaState(std::string id, std::string address);
  bool observe_latency(double latency_us, double smoothing);
  bool observe_completion(std::chrono::steady_clock::time_point completed_at,
                          double latency_us, bool success, bool timeout,
                          bool deadline_missed, bool cancelled,
                          double smoothing);
  bool record_censored_latency_lower_bound(
      double latency_us,
      std::chrono::steady_clock::time_point observed_at =
          std::chrono::steady_clock::now());
  void record_routed() noexcept;
  [[nodiscard]] ReplicaStats stats(
      std::chrono::steady_clock::time_point now =
          std::chrono::steady_clock::now()) const;
  [[nodiscard]] ReplicaWindow take_window();
  void set_health(HealthState health) noexcept;

  const std::string id;
  const std::string address;
  std::atomic<std::uint64_t> inflight{0};

 private:
  friend class Selector;
  friend class EwmaLatencySelector;
  friend class P2CLatencyInflightSelector;
  mutable std::mutex observation_mutex_;
  struct CensoredLatencySample {
    double lower_bound_us{0.0};
    std::chrono::steady_clock::time_point observed_at{};
  };
  double latency_ewma_us_{0.0};
  std::uint64_t latency_samples_{0};
  double error_ewma_{0.0};
  std::array<double, kLatencyWindowCapacity> latency_window_{};
  std::size_t latency_window_size_{0};
  std::size_t latency_window_next_{0};
  std::array<CensoredLatencySample, kLatencyWindowCapacity>
      censored_latency_window_{};
  std::size_t censored_latency_window_size_{0};
  std::size_t censored_latency_window_next_{0};
  std::array<double, kLatencyWindowCapacity> control_latency_window_{};
  std::size_t control_latency_window_size_{0};
  std::size_t control_latency_window_next_{0};
  std::uint64_t completed_{0};
  std::uint64_t succeeded_{0};
  std::uint64_t failed_{0};
  std::uint64_t timed_out_{0};
  std::uint64_t routed_{0};
  std::uint64_t window_completed_{0};
  std::uint64_t window_control_samples_{0};
  std::uint64_t window_succeeded_{0};
  std::uint64_t window_useful_successes_{0};
  std::uint64_t window_failed_{0};
  std::uint64_t window_timed_out_{0};
  std::uint64_t window_deadline_missed_{0};
  std::uint64_t consecutive_failures_{0};
  std::chrono::steady_clock::time_point last_success_{};
  std::chrono::steady_clock::time_point last_failure_{};
  bool has_success_{false};
  bool has_failure_{false};
  HealthState health_{HealthState::kHealthy};
};

class ReplicaLease {
 public:
  explicit ReplicaLease(std::shared_ptr<ReplicaState> replica);
  ~ReplicaLease();
  ReplicaLease(ReplicaLease&& other) noexcept;
  ReplicaLease& operator=(ReplicaLease&& other) noexcept;
  ReplicaLease(const ReplicaLease&) = delete;
  ReplicaLease& operator=(const ReplicaLease&) = delete;

  [[nodiscard]] const std::shared_ptr<ReplicaState>& replica() const noexcept;
  void release() noexcept;

 private:
  std::shared_ptr<ReplicaState> replica_;
};

class Selector {
 public:
  virtual ~Selector() = default;
  [[nodiscard]] ReplicaLease select(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas);
  [[nodiscard]] virtual std::string_view name() const noexcept = 0;
  virtual void record_latency(const std::shared_ptr<ReplicaState>& replica,
                              double latency_us);

 protected:
  [[nodiscard]] virtual std::size_t choose_locked(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas) = 0;
  std::mutex selection_mutex_;
};

class RoundRobinSelector final : public Selector {
 public:
  explicit RoundRobinSelector(std::uint64_t first_index = 0) noexcept;
  [[nodiscard]] std::string_view name() const noexcept override;

 protected:
  [[nodiscard]] std::size_t choose_locked(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas) override;

 private:
  std::uint64_t next_;
};

class LeastInflightSelector final : public Selector {
 public:
  [[nodiscard]] std::string_view name() const noexcept override;

 protected:
  [[nodiscard]] std::size_t choose_locked(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas) override;
};

class EwmaLatencySelector final : public Selector {
 public:
  explicit EwmaLatencySelector(double smoothing);
  [[nodiscard]] std::string_view name() const noexcept override;
  void record_latency(const std::shared_ptr<ReplicaState>& replica,
                      double latency_us) override;

 protected:
  [[nodiscard]] std::size_t choose_locked(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas) override;

 private:
  double smoothing_;
  std::uint64_t cold_start_index_{0};
};

class P2CLatencyInflightSelector final : public Selector {
 public:
  P2CLatencyInflightSelector(std::uint64_t seed, double smoothing);
  [[nodiscard]] std::string_view name() const noexcept override;
  void record_latency(const std::shared_ptr<ReplicaState>& replica,
                      double latency_us) override;

 protected:
  [[nodiscard]] std::size_t choose_locked(
      const std::vector<std::shared_ptr<ReplicaState>>& replicas) override;

 private:
  [[nodiscard]] double score(const ReplicaState& replica) const;
  std::uint64_t random_state_;
  double smoothing_;
};

enum class Policy {
  kRoundRobin,
  kLeastInflight,
  kEwmaLatency,
  kP2CLatencyInflight,
  kArtcSelectorOnly,
  kAdaptiveConcurrencyOnly,
  kArtcAdaptiveNoDeadline,
  kArtcAdaptive,
};

[[nodiscard]] Policy parse_policy(std::string_view value);
[[nodiscard]] std::unique_ptr<Selector> make_selector(
    Policy policy, std::uint64_t seed, double smoothing = 0.2);

}  // namespace artc::routing
