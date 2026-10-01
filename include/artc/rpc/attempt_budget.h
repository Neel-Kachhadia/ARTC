#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <chrono>

namespace artc::rpc {

struct HedgeBudgetTag;
struct RetryBudgetTag;

struct AttemptBudgetConfig {
  std::uint32_t capacity{0};
  double refill_per_second{0.0};
};

inline constexpr std::uint32_t kHardMaxAttemptBudgetCapacity = 1'000'000;

struct AttemptBudgetSnapshot {
  double tokens{0.0};
  std::uint32_t available{0};
  std::uint64_t consumed_total{0};
  std::uint64_t denied_total{0};
};

template <typename Tag>
class TokenBudget final {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  explicit TokenBudget(AttemptBudgetConfig config,
                       TimePoint now = Clock::now())
      : config_(config), tokens_(static_cast<double>(config.capacity)),
        last_refill_(now) {
    static_assert(std::is_same_v<Tag, HedgeBudgetTag> ||
                  std::is_same_v<Tag, RetryBudgetTag>);
    if (config_.capacity > kHardMaxAttemptBudgetCapacity) {
      throw std::invalid_argument("attempt budget capacity exceeds hard limit");
    }
    if (!std::isfinite(config_.refill_per_second) ||
        config_.refill_per_second < 0.0) {
      throw std::invalid_argument("attempt budget refill must be finite and non-negative");
    }
  }

  [[nodiscard]] bool try_consume(TimePoint now = Clock::now()) noexcept {
    std::lock_guard lock(mutex_);
    refill_locked(now);
    if (tokens_ < 1.0) {
      increment_saturated(&denied_total_);
      return false;
    }
    tokens_ -= 1.0;
    increment_saturated(&consumed_total_);
    return true;
  }

  [[nodiscard]] AttemptBudgetSnapshot snapshot(
      TimePoint now = Clock::now()) noexcept {
    std::lock_guard lock(mutex_);
    refill_locked(now);
    return {tokens_, static_cast<std::uint32_t>(tokens_), consumed_total_,
            denied_total_};
  }

 private:
  static void increment_saturated(std::uint64_t* value) noexcept {
    if (*value != UINT64_MAX) ++*value;
  }

  void refill_locked(TimePoint now) noexcept {
    if (now <= last_refill_) return;
    const long double now_seconds =
        std::chrono::duration<long double>(now.time_since_epoch()).count();
    const long double last_refill_seconds =
        std::chrono::duration<long double>(last_refill_.time_since_epoch()).count();
    const long double elapsed_seconds = now_seconds - last_refill_seconds;
    const long double added = elapsed_seconds * config_.refill_per_second;
    const long double capped = std::min(
        static_cast<long double>(config_.capacity),
        static_cast<long double>(tokens_) + added);
    tokens_ = static_cast<double>(capped);
    last_refill_ = now;
  }

  const AttemptBudgetConfig config_;
  std::mutex mutex_;
  double tokens_;
  TimePoint last_refill_;
  std::uint64_t consumed_total_{0};
  std::uint64_t denied_total_{0};
};

using HedgeBudget = TokenBudget<HedgeBudgetTag>;
using RetryBudget = TokenBudget<RetryBudgetTag>;

}  // namespace artc::rpc
