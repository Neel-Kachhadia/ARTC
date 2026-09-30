#include "artc/rpc/attempt_policy.h"

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

namespace artc::rpc {
namespace {

using namespace std::chrono_literals;

TEST(AttemptPolicyTest, UnknownMethodsDefaultToNonIdempotentWithoutDuplicates) {
  const std::unordered_map<std::string, MethodPolicy> policies{
      {"/artc.v1.Traffic/Execute", MethodPolicy{}}};

  const auto unknown = resolve_method_policy("/unknown", policies);
  EXPECT_EQ(unknown.idempotency, Idempotency::kNonIdempotent);
  EXPECT_FALSE(unknown.hedging_enabled);
  EXPECT_FALSE(unknown.retry_enabled);
  EXPECT_EQ(unknown.max_total_attempts, 1U);
  EXPECT_EQ(unknown.max_retries, 0U);
  EXPECT_FALSE(is_retryable(unknown, grpc::StatusCode::UNAVAILABLE));
  EXPECT_EQ(resolve_method_policy("/artc.v1.Traffic/Execute", policies)
                .idempotency,
            Idempotency::kNonIdempotent);
}

TEST(AttemptPolicyTest, AcceptsValidIdempotentRetryAndHedgePolicies) {
  MethodPolicy retry;
  retry.idempotency = Idempotency::kIdempotent;
  retry.retry_enabled = true;
  retry.max_total_attempts = 3;
  retry.max_retries = 2;
  validate(retry);
  EXPECT_TRUE(is_retryable(retry, grpc::StatusCode::UNAVAILABLE));
  EXPECT_FALSE(is_retryable(retry, grpc::StatusCode::INVALID_ARGUMENT));
  EXPECT_FALSE(is_retryable(retry, grpc::StatusCode::OK));
  EXPECT_FALSE(is_retryable(retry, grpc::StatusCode::DO_NOT_USE));

  MethodPolicy hedge;
  hedge.idempotency = Idempotency::kIdempotent;
  hedge.hedging_enabled = true;
  hedge.max_total_attempts = 2;
  validate(hedge);
}

TEST(AttemptPolicyTest, RejectsInvalidIdempotencyAndAttemptBounds) {
  MethodPolicy policy;
  policy.idempotency = static_cast<Idempotency>(99);
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.max_total_attempts = 0;
  EXPECT_THROW(validate(policy), std::invalid_argument);
  policy.max_total_attempts = kHardMaxTotalAttempts + 1;
  EXPECT_THROW(validate(policy), std::invalid_argument);
}

TEST(AttemptPolicyTest, RejectsInconsistentRetryAndHedgeConfiguration) {
  MethodPolicy policy;
  policy.retry_enabled = true;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.max_retries = 1;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.retry_enabled = true;
  policy.max_retries = 2;
  policy.max_total_attempts = 2;
  policy.idempotency = Idempotency::kIdempotent;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.idempotency = Idempotency::kIdempotent;
  policy.retry_enabled = true;
  policy.max_total_attempts = 2;
  policy.max_retries = 1;
  policy.retryable_statuses.clear();
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.hedging_enabled = true;
  EXPECT_THROW(validate(policy), std::invalid_argument);
}

TEST(AttemptPolicyTest, RejectsAutomaticDuplicatesForNonIdempotentMethods) {
  MethodPolicy policy;
  policy.hedging_enabled = true;
  policy.max_total_attempts = 2;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.retry_enabled = true;
  policy.max_total_attempts = 2;
  policy.max_retries = 1;
  EXPECT_THROW(validate(policy), std::invalid_argument);
}

TEST(AttemptPolicyTest, RejectsInvalidRetryStatusesAndBackoffRanges) {
  MethodPolicy policy;
  policy.retryable_statuses = {grpc::StatusCode::OK};
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy.retryable_statuses = {grpc::StatusCode::UNAVAILABLE,
                               grpc::StatusCode::UNAVAILABLE};
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy.retryable_statuses = {grpc::StatusCode::DO_NOT_USE};
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy.retryable_statuses = {grpc::StatusCode::UNAVAILABLE};
  policy.hedge_delay_min = 2ms;
  policy.hedge_delay_max = 1ms;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.retry_backoff_base = 20ms;
  policy.retry_backoff_max = 10ms;
  EXPECT_THROW(validate(policy), std::invalid_argument);

  policy = MethodPolicy{};
  policy.retry_jitter_max = policy.retry_backoff_max + 1ms;
  EXPECT_THROW(validate(policy), std::invalid_argument);
}

TEST(AttemptPolicyTest, ClampsP95HedgeDelayAndUsesMaximumWithoutSamples) {
  MethodPolicy policy;
  policy.hedging_enabled = true;
  policy.idempotency = Idempotency::kIdempotent;
  policy.max_total_attempts = 2;

  EXPECT_EQ(hedge_delay_for_p95(policy, std::nullopt), 100ms);
  EXPECT_EQ(hedge_delay_for_p95(policy, 5ms), 10ms);
  EXPECT_EQ(hedge_delay_for_p95(policy, 45ms), 45ms);
  EXPECT_EQ(hedge_delay_for_p95(policy, 500ms), 100ms);
  EXPECT_THROW(hedge_delay_for_p95(policy, -1us), std::invalid_argument);
}

TEST(AttemptPolicyTest, BackoffUsesInjectedJitterAndSaturatesWithoutOverflow) {
  EXPECT_EQ(bounded_exponential_backoff(10ms, 100ms, 0, 5ms), 15ms);
  EXPECT_EQ(bounded_exponential_backoff(10ms, 100ms, 2, 5ms), 45ms);
  EXPECT_EQ(bounded_exponential_backoff(60ms, 100ms, 1, 0ms), 100ms);
  EXPECT_EQ(bounded_exponential_backoff(
                1ms, std::chrono::milliseconds::max(),
                std::numeric_limits<std::uint32_t>::max(), 0ms),
            std::chrono::milliseconds::max());
  EXPECT_EQ(bounded_exponential_backoff(
                10ms, 100ms, 0, std::chrono::milliseconds::max()),
            100ms);
  EXPECT_THROW(bounded_exponential_backoff(0ms, 100ms, 0, 0ms),
               std::invalid_argument);
  EXPECT_THROW(bounded_exponential_backoff(10ms, 100ms, 0, -1ms),
               std::invalid_argument);
}

TEST(AttemptPolicyTest, AppliesConfiguredRetryCountAndJitterBound) {
  MethodPolicy policy;
  policy.idempotency = Idempotency::kIdempotent;
  policy.retry_enabled = true;
  policy.max_total_attempts = 3;
  policy.max_retries = 2;

  EXPECT_EQ(retry_backoff_delay(policy, 0, 5ms), 15ms);
  EXPECT_EQ(retry_backoff_delay(policy, 1, 10ms), 30ms);
  EXPECT_THROW(retry_backoff_delay(policy, 2, 0ms), std::invalid_argument);
  EXPECT_THROW(retry_backoff_delay(policy, 0, 11ms), std::invalid_argument);
}

TEST(AttemptPolicyTest, ChecksRetryDelayAndMinimumAttemptBudgetAgainstDeadline) {
  EXPECT_FALSE(retry_delay_fits_deadline(100ms, 25ms, 75ms));
  EXPECT_TRUE(retry_delay_fits_deadline(100ms + 1ns, 25ms, 75ms));
  EXPECT_TRUE(retry_delay_fits_deadline(100ms, 25ms, 50ms));
  EXPECT_FALSE(retry_delay_fits_deadline(100ms, 25ms, 76ms));
  EXPECT_FALSE(retry_delay_fits_deadline(25ms, 25ms));
  EXPECT_FALSE(retry_delay_fits_deadline(-1ms, 0ms));
  EXPECT_FALSE(retry_delay_fits_deadline(100ms, -1ms));
}

}  // namespace
}  // namespace artc::rpc
