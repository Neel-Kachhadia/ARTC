#include "artc/routing/selector.h"

#include <cmath>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>

namespace artc::routing {

ReplicaState::ReplicaState(std::string replica_id, std::string replica_address)
    : id(std::move(replica_id)), address(std::move(replica_address)) {
  if (id.empty() || address.empty()) {
    throw std::invalid_argument("replica id and address must be non-empty");
  }
}

bool ReplicaState::observe_latency(double latency_us, double smoothing) {
  if (!std::isfinite(latency_us) || latency_us < 0.0 ||
      !std::isfinite(smoothing) || smoothing <= 0.0 || smoothing > 1.0) {
    return false;
  }
  std::lock_guard lock(observation_mutex_);
  latency_ewma_us_ = latency_samples_ == 0
                         ? latency_us
                         : smoothing * latency_us + (1.0 - smoothing) * latency_ewma_us_;
  ++latency_samples_;
  return true;
}

ReplicaLease::ReplicaLease(std::shared_ptr<ReplicaState> replica)
    : replica_(std::move(replica)) {
  if (!replica_) throw std::invalid_argument("replica lease requires a replica");
  auto current = replica_->inflight.load(std::memory_order_relaxed);
  do {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error("replica inflight count overflow");
    }
  } while (!replica_->inflight.compare_exchange_weak(
      current, current + 1, std::memory_order_relaxed, std::memory_order_relaxed));
}

ReplicaLease::~ReplicaLease() { release(); }

ReplicaLease::ReplicaLease(ReplicaLease&& other) noexcept
    : replica_(std::move(other.replica_)) {}

ReplicaLease& ReplicaLease::operator=(ReplicaLease&& other) noexcept {
  if (this != &other) {
    release();
    replica_ = std::move(other.replica_);
  }
  return *this;
}

const std::shared_ptr<ReplicaState>& ReplicaLease::replica() const noexcept {
  return replica_;
}

void ReplicaLease::release() noexcept {
  if (!replica_) return;
  auto current = replica_->inflight.load(std::memory_order_relaxed);
  while (current != 0 && !replica_->inflight.compare_exchange_weak(
                             current, current - 1, std::memory_order_release,
                             std::memory_order_relaxed)) {}
  if (current == 0) std::terminate();
  replica_.reset();
}

ReplicaLease Selector::select(
    const std::vector<std::shared_ptr<ReplicaState>>& replicas) {
  if (replicas.empty()) throw std::invalid_argument("replica pool is empty");
  for (const auto& replica : replicas) {
    if (!replica) throw std::invalid_argument("replica pool contains null target");
  }
  std::lock_guard lock(selection_mutex_);
  const std::size_t selected = choose_locked(replicas);
  if (selected >= replicas.size()) {
    throw std::logic_error("selector returned an invalid replica");
  }
  return ReplicaLease(replicas[selected]);
}

void Selector::record_latency(const std::shared_ptr<ReplicaState>&, double) {}

RoundRobinSelector::RoundRobinSelector(std::uint64_t first_index) noexcept
    : next_(first_index) {}

std::string_view RoundRobinSelector::name() const noexcept { return "round_robin"; }

std::size_t RoundRobinSelector::choose_locked(
    const std::vector<std::shared_ptr<ReplicaState>>& replicas) {
  const auto count = static_cast<std::uint64_t>(replicas.size());
  const auto index = static_cast<std::size_t>(next_ % count);
  next_ = static_cast<std::uint64_t>(index) + 1U;
  if (next_ == count) next_ = 0;
  return index;
}

std::string_view LeastInflightSelector::name() const noexcept {
  return "least_inflight";
}

std::size_t LeastInflightSelector::choose_locked(
    const std::vector<std::shared_ptr<ReplicaState>>& replicas) {
  std::size_t selected = 0;
  std::uint64_t lowest = replicas[0]->inflight.load(std::memory_order_relaxed);
  for (std::size_t i = 1; i < replicas.size(); ++i) {
    const auto inflight = replicas[i]->inflight.load(std::memory_order_relaxed);
    if (inflight < lowest) {
      selected = i;
      lowest = inflight;
    }
  }
  return selected;
}

EwmaLatencySelector::EwmaLatencySelector(double smoothing) : smoothing_(smoothing) {
  if (!std::isfinite(smoothing_) || smoothing_ <= 0.0 || smoothing_ > 1.0) {
    throw std::invalid_argument("EWMA smoothing must be in (0, 1]");
  }
}

std::string_view EwmaLatencySelector::name() const noexcept { return "ewma_latency"; }

void EwmaLatencySelector::record_latency(
    const std::shared_ptr<ReplicaState>& replica, double latency_us) {
  if (!replica || !replica->observe_latency(latency_us, smoothing_)) {
    throw std::invalid_argument("invalid EWMA latency observation");
  }
}

std::size_t EwmaLatencySelector::choose_locked(
    const std::vector<std::shared_ptr<ReplicaState>>& replicas) {
  for (std::size_t offset = 0; offset < replicas.size(); ++offset) {
    const auto start = static_cast<std::size_t>(cold_start_index_ % replicas.size());
    const std::size_t index = (start + offset) % replicas.size();
    std::lock_guard observation_lock(replicas[index]->observation_mutex_);
    if (replicas[index]->latency_samples_ == 0) {
      ++cold_start_index_;
      return index;
    }
  }

  std::size_t selected = 0;
  double best_score = std::numeric_limits<double>::infinity();
  for (std::size_t i = 0; i < replicas.size(); ++i) {
    std::lock_guard observation_lock(replicas[i]->observation_mutex_);
    const double latency = replicas[i]->latency_ewma_us_;
    const double inflight = static_cast<double>(
        replicas[i]->inflight.load(std::memory_order_relaxed));
    const double candidate = latency * (1.0 + inflight);
    if (candidate < best_score) {
      selected = i;
      best_score = candidate;
    }
  }
  return selected;
}

P2CLatencyInflightSelector::P2CLatencyInflightSelector(
    std::uint64_t seed, double smoothing)
    : random_state_(seed), smoothing_(smoothing) {
  if (!std::isfinite(smoothing_) || smoothing_ <= 0.0 || smoothing_ > 1.0) {
    throw std::invalid_argument("EWMA smoothing must be in (0, 1]");
  }
}

std::string_view P2CLatencyInflightSelector::name() const noexcept {
  return "p2c_latency_inflight";
}

void P2CLatencyInflightSelector::record_latency(
    const std::shared_ptr<ReplicaState>& replica, double latency_us) {
  if (!replica || !replica->observe_latency(latency_us, smoothing_)) {
    throw std::invalid_argument("invalid P2C latency observation");
  }
}

double P2CLatencyInflightSelector::score(const ReplicaState& replica) const {
  std::lock_guard observation_lock(replica.observation_mutex_);
  const double latency = replica.latency_samples_ == 0 ? 1.0 : replica.latency_ewma_us_;
  const double inflight = static_cast<double>(
      replica.inflight.load(std::memory_order_relaxed));
  return latency * (1.0 + inflight);
}

std::size_t P2CLatencyInflightSelector::choose_locked(
    const std::vector<std::shared_ptr<ReplicaState>>& replicas) {
  if (replicas.size() == 1) return 0;
  // ponytail: selection serializes on one mutex; shard only if profiling shows contention.
  auto next_random = [this]() {
    random_state_ += UINT64_C(0x9e3779b97f4a7c15);
    std::uint64_t value = random_state_;
    value = (value ^ (value >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27U)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
  };
  const auto count = static_cast<std::uint64_t>(replicas.size());
  const std::size_t first = static_cast<std::size_t>(next_random() % count);
  std::size_t second = static_cast<std::size_t>(next_random() % (count - 1));
  if (second >= first) ++second;
  return score(*replicas[first]) <= score(*replicas[second]) ? first : second;
}

Policy parse_policy(std::string_view value) {
  if (value == "round_robin") return Policy::kRoundRobin;
  if (value == "least_inflight") return Policy::kLeastInflight;
  if (value == "ewma_latency") return Policy::kEwmaLatency;
  if (value == "p2c_latency_inflight") return Policy::kP2CLatencyInflight;
  throw std::invalid_argument("unknown routing policy");
}

std::unique_ptr<Selector> make_selector(Policy policy, std::uint64_t seed,
                                        double smoothing) {
  switch (policy) {
    case Policy::kRoundRobin:
      return std::make_unique<RoundRobinSelector>();
    case Policy::kLeastInflight:
      return std::make_unique<LeastInflightSelector>();
    case Policy::kEwmaLatency:
      return std::make_unique<EwmaLatencySelector>(smoothing);
    case Policy::kP2CLatencyInflight:
      return std::make_unique<P2CLatencyInflightSelector>(seed, smoothing);
  }
  throw std::invalid_argument("invalid routing policy");
}

}  // namespace artc::routing
