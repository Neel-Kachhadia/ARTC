#pragma once

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace artc::rpc {

enum class Idempotency { kNonIdempotent, kIdempotent };
enum class AttemptKind { kPrimary, kHedge, kRetry };
enum class AttemptState {
  kCreated,
  kInFlight,
  kSucceeded,
  kFailed,
  kTimedOut,
  kCancelled,
};

struct MethodPolicy {
  Idempotency idempotency{Idempotency::kNonIdempotent};
  bool hedging_enabled{false};
  bool retry_enabled{false};
  bool allow_same_replica_retry{false};
  std::uint32_t max_total_attempts{1};
  std::uint32_t max_retries{0};
  std::chrono::microseconds hedge_delay_min{10'000};
  std::chrono::microseconds hedge_delay_max{100'000};
  std::chrono::milliseconds retry_backoff_base{10};
  std::chrono::milliseconds retry_backoff_max{100};
  std::chrono::milliseconds retry_jitter_max{10};
  std::vector<grpc::StatusCode> retryable_statuses{
      grpc::StatusCode::UNAVAILABLE};
};

inline constexpr std::uint32_t kHardMaxTotalAttempts = 3;

inline bool is_valid(Idempotency idempotency) noexcept {
  return idempotency == Idempotency::kNonIdempotent ||
         idempotency == Idempotency::kIdempotent;
}

inline bool is_valid_status_code(grpc::StatusCode status) noexcept {
  switch (status) {
    case grpc::StatusCode::OK:
    case grpc::StatusCode::CANCELLED:
    case grpc::StatusCode::UNKNOWN:
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
    case grpc::StatusCode::NOT_FOUND:
    case grpc::StatusCode::ALREADY_EXISTS:
    case grpc::StatusCode::PERMISSION_DENIED:
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
    case grpc::StatusCode::FAILED_PRECONDITION:
    case grpc::StatusCode::ABORTED:
    case grpc::StatusCode::OUT_OF_RANGE:
    case grpc::StatusCode::UNIMPLEMENTED:
    case grpc::StatusCode::INTERNAL:
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::DATA_LOSS:
    case grpc::StatusCode::UNAUTHENTICATED:
      return true;
    case grpc::StatusCode::DO_NOT_USE:
      return false;
  }
  return false;
}

inline void validate(const MethodPolicy& policy) {
  if (!is_valid(policy.idempotency)) {
    throw std::invalid_argument("invalid method idempotency");
  }
  if (policy.max_total_attempts == 0 ||
      policy.max_total_attempts > kHardMaxTotalAttempts) {
    throw std::invalid_argument("max_total_attempts must be between 1 and 3");
  }
  if (policy.max_retries > policy.max_total_attempts - 1) {
    throw std::invalid_argument("max_retries exceeds max_total_attempts");
  }
  if (policy.retry_enabled != (policy.max_retries != 0)) {
    throw std::invalid_argument("retry_enabled and max_retries disagree");
  }
  if (policy.retry_enabled && policy.retryable_statuses.empty()) {
    throw std::invalid_argument("retry_enabled requires retryable statuses");
  }
  if (policy.hedging_enabled && policy.max_total_attempts < 2) {
    throw std::invalid_argument("hedging requires at least two total attempts");
  }
  if (policy.hedge_delay_min < std::chrono::microseconds::zero() ||
      policy.hedge_delay_max < policy.hedge_delay_min ||
      policy.hedge_delay_max > std::chrono::seconds(60) ||
      (policy.hedging_enabled &&
       policy.hedge_delay_min == std::chrono::microseconds::zero())) {
    throw std::invalid_argument("invalid hedge delay range");
  }
  if (policy.retry_backoff_base <= std::chrono::milliseconds::zero() ||
      policy.retry_backoff_max < policy.retry_backoff_base ||
      policy.retry_jitter_max < std::chrono::milliseconds::zero() ||
      policy.retry_jitter_max > policy.retry_backoff_max ||
      policy.retry_backoff_max > std::chrono::seconds(60)) {
    throw std::invalid_argument("invalid retry backoff range");
  }
  if (policy.idempotency == Idempotency::kNonIdempotent &&
      (policy.hedging_enabled || policy.retry_enabled)) {
    throw std::invalid_argument(
        "non-idempotent methods cannot hedge or retry automatically");
  }
  for (std::size_t i = 0; i < policy.retryable_statuses.size(); ++i) {
    if (!is_valid_status_code(policy.retryable_statuses[i]) ||
        policy.retryable_statuses[i] == grpc::StatusCode::OK) {
      throw std::invalid_argument("retryable_statuses contains an invalid status");
    }
    if (std::find(policy.retryable_statuses.begin(),
                  policy.retryable_statuses.begin() +
                      static_cast<std::ptrdiff_t>(i),
                  policy.retryable_statuses[i]) !=
        policy.retryable_statuses.begin() + static_cast<std::ptrdiff_t>(i)) {
      throw std::invalid_argument("retryable_statuses contains duplicates");
    }
  }
}

// Use the p95 estimate when available; with no observations, wait the maximum.
inline std::chrono::microseconds hedge_delay_for_p95(
    const MethodPolicy& policy,
    std::optional<std::chrono::microseconds> p95_latency) {
  validate(policy);
  if (!p95_latency) return policy.hedge_delay_max;
  if (*p95_latency < std::chrono::microseconds::zero()) {
    throw std::invalid_argument("p95 latency must not be negative");
  }
  return std::clamp(*p95_latency, policy.hedge_delay_min,
                    policy.hedge_delay_max);
}

// retry_index is zero-based: the first retry uses base + jitter.
inline std::chrono::milliseconds bounded_exponential_backoff(
    std::chrono::milliseconds base, std::chrono::milliseconds maximum,
    std::uint32_t retry_index, std::chrono::milliseconds jitter) {
  if (base <= std::chrono::milliseconds::zero() || maximum < base ||
      jitter < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument("invalid retry backoff inputs");
  }

  auto delay = base.count();
  const auto cap = maximum.count();
  for (std::uint32_t index = 0; index < retry_index && delay < cap; ++index) {
    if (delay > cap / 2) {
      delay = cap;
      break;
    }
    delay *= 2;
  }
  if (jitter.count() >= cap - delay) return maximum;
  return std::chrono::milliseconds(delay + jitter.count());
}

inline std::chrono::milliseconds retry_backoff_delay(
    const MethodPolicy& policy, std::uint32_t retry_index,
    std::chrono::milliseconds jitter) {
  validate(policy);
  if (!policy.retry_enabled || retry_index >= policy.max_retries) {
    throw std::invalid_argument("retry index is outside the configured policy");
  }
  if (jitter < std::chrono::milliseconds::zero() ||
      jitter > policy.retry_jitter_max) {
    throw std::invalid_argument("retry jitter exceeds the configured bound");
  }
  return bounded_exponential_backoff(policy.retry_backoff_base,
                                     policy.retry_backoff_max, retry_index,
                                     jitter);
}

inline bool retry_delay_fits_deadline(
    std::chrono::nanoseconds remaining,
    std::chrono::nanoseconds delay,
    std::chrono::nanoseconds minimum_attempt_budget =
        std::chrono::nanoseconds::zero()) noexcept {
  if (remaining <= std::chrono::nanoseconds::zero() ||
      delay < std::chrono::nanoseconds::zero() ||
      minimum_attempt_budget < std::chrono::nanoseconds::zero() ||
      remaining < delay) {
    return false;
  }
  const auto after_delay = remaining - delay;
  return after_delay > std::chrono::nanoseconds::zero() &&
         after_delay > minimum_attempt_budget;
}

inline MethodPolicy resolve_method_policy(
    std::string_view method,
    const std::unordered_map<std::string, MethodPolicy>& policies) {
  const auto found = policies.find(std::string(method));
  if (found == policies.end()) return {};
  return found->second;
}

inline bool is_retryable(const MethodPolicy& policy,
                         grpc::StatusCode status) noexcept {
  return policy.retry_enabled && is_valid_status_code(status) &&
         status != grpc::StatusCode::OK &&
         std::find(policy.retryable_statuses.begin(),
                   policy.retryable_statuses.end(), status) !=
             policy.retryable_statuses.end();
}

}  // namespace artc::rpc
