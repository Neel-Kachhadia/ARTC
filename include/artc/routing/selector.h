#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace artc::routing {

struct ReplicaState {
  ReplicaState(std::string id, std::string address);
  bool observe_latency(double latency_us, double smoothing);

  const std::string id;
  const std::string address;
  std::atomic<std::uint64_t> inflight{0};

 private:
  friend class Selector;
  friend class EwmaLatencySelector;
  friend class P2CLatencyInflightSelector;
  mutable std::mutex observation_mutex_;
  double latency_ewma_us_{0.0};
  std::uint64_t latency_samples_{0};
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

enum class Policy { kRoundRobin, kLeastInflight, kEwmaLatency, kP2CLatencyInflight };

[[nodiscard]] Policy parse_policy(std::string_view value);
[[nodiscard]] std::unique_ptr<Selector> make_selector(
    Policy policy, std::uint64_t seed, double smoothing = 0.2);

}  // namespace artc::routing
