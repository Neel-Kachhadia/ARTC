#include "artc/rpc/services.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

extern char** environ;

namespace {

constexpr std::string_view kKnownRouterEnvironment[]{
    "ARTC_AIMD_MIN_LIMIT", "ARTC_AIMD_MAX_LIMIT", "ARTC_AIMD_INITIAL_LIMIT",
    "ARTC_AIMD_ALPHA", "ARTC_AIMD_BETA", "ARTC_AIMD_INTERVAL_MS",
    "ARTC_AIMD_MIN_SAMPLES", "ARTC_AIMD_TARGET_LATENCY_US",
    "ARTC_AIMD_OVERLOAD_ERROR_FRACTION", "ARTC_DEADLINE_MARGIN_US",
    "ARTC_DEFAULT_DEADLINE_MS", "ARTC_DECISION_SAMPLE_EVERY",
    "ARTC_HEALTH_MIN_SAMPLES", "ARTC_HEALTH_FAILURES",
    "ARTC_HEALTH_RECOVERY_SUCCESSES", "ARTC_HEALTH_RECOVERY_COOLDOWN_MS",
    "ARTC_HEALTH_DEGRADED_RATIO", "ARTC_HEALTH_RECOVERED_RATIO",
    "ARTC_RECOVERY_PROBE_PERIOD", "ARTC_ATTEMPT_MAX_TOTAL",
    "ARTC_ATTEMPT_MAX_ACTIVE", "ARTC_ATTEMPT_JITTER_SEED",
    "ARTC_ATTEMPT_MINIMUM_BUDGET_US", "ARTC_HEDGE_BUDGET_CAPACITY",
    "ARTC_HEDGE_BUDGET_REFILL_PER_SECOND", "ARTC_RETRY_BUDGET_CAPACITY",
    "ARTC_RETRY_BUDGET_REFILL_PER_SECOND", "ARTC_EXECUTE_IDEMPOTENCY",
    "ARTC_EXECUTE_HEDGING_ENABLED", "ARTC_EXECUTE_RETRY_ENABLED",
    "ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY", "ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS",
    "ARTC_EXECUTE_MAX_RETRIES", "ARTC_EXECUTE_HEDGE_DELAY_MIN_US",
    "ARTC_EXECUTE_HEDGE_DELAY_MAX_US", "ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS",
    "ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS", "ARTC_EXECUTE_RETRY_JITTER_MAX_MS",
    "ARTC_EXECUTE_RETRYABLE_STATUSES", "ARTC_ROUTING_POLICY", "ARTC_RUN_ID",
    "ARTC_GIT_REVISION", "ARTC_WORKTREE_DIRTY", "ARTC_COMPOSE_PROJECT",
};

void validate_router_environment() {
  for (char** entry = ::environ; entry != nullptr && *entry != nullptr; ++entry) {
    const std::string_view assignment(*entry);
    const auto separator = assignment.find('=');
    const auto name = assignment.substr(0, separator);
    if (!name.starts_with("ARTC_")) continue;
    if (std::find(std::begin(kKnownRouterEnvironment),
                  std::end(kKnownRouterEnvironment), name) ==
        std::end(kKnownRouterEnvironment)) {
      throw std::invalid_argument("unknown ARTC router configuration variable");
    }
  }
}

template <typename T>
T parse_integer(std::string_view value) {
  T result{};
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("invalid integer argument");
  }
  return result;
}

double parse_double(std::string_view value) {
  double result = 0.0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      !std::isfinite(result)) {
    throw std::invalid_argument("invalid floating-point argument");
  }
  return result;
}

template <typename T>
T environment_integer(const char* name, T fallback) {
  const char* value = std::getenv(name);
  return value == nullptr ? fallback : parse_integer<T>(value);
}

double environment_double(const char* name, double fallback) {
  const char* value = std::getenv(name);
  return value == nullptr ? fallback : parse_double(value);
}

bool environment_bool(const char* name, bool fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr) return fallback;
  const std::string_view parsed(value);
  if (parsed == "true") return true;
  if (parsed == "false") return false;
  throw std::invalid_argument(std::string(name) + " must be exactly true or false");
}

std::vector<grpc::StatusCode> retryable_statuses_from_environment() {
  const char* value = std::getenv("ARTC_EXECUTE_RETRYABLE_STATUSES");
  if (value == nullptr) return {grpc::StatusCode::UNAVAILABLE};

  static constexpr std::pair<std::string_view, grpc::StatusCode> kStatusNames[]{
      {"OK", grpc::StatusCode::OK},
      {"CANCELLED", grpc::StatusCode::CANCELLED},
      {"UNKNOWN", grpc::StatusCode::UNKNOWN},
      {"INVALID_ARGUMENT", grpc::StatusCode::INVALID_ARGUMENT},
      {"DEADLINE_EXCEEDED", grpc::StatusCode::DEADLINE_EXCEEDED},
      {"NOT_FOUND", grpc::StatusCode::NOT_FOUND},
      {"ALREADY_EXISTS", grpc::StatusCode::ALREADY_EXISTS},
      {"PERMISSION_DENIED", grpc::StatusCode::PERMISSION_DENIED},
      {"RESOURCE_EXHAUSTED", grpc::StatusCode::RESOURCE_EXHAUSTED},
      {"FAILED_PRECONDITION", grpc::StatusCode::FAILED_PRECONDITION},
      {"ABORTED", grpc::StatusCode::ABORTED},
      {"OUT_OF_RANGE", grpc::StatusCode::OUT_OF_RANGE},
      {"UNIMPLEMENTED", grpc::StatusCode::UNIMPLEMENTED},
      {"INTERNAL", grpc::StatusCode::INTERNAL},
      {"UNAVAILABLE", grpc::StatusCode::UNAVAILABLE},
      {"DATA_LOSS", grpc::StatusCode::DATA_LOSS},
      {"UNAUTHENTICATED", grpc::StatusCode::UNAUTHENTICATED},
      {"DO_NOT_USE", grpc::StatusCode::DO_NOT_USE},
  };

  std::vector<grpc::StatusCode> statuses;
  std::string_view remaining(value);
  while (true) {
    const auto separator = remaining.find(',');
    const auto name = remaining.substr(0, separator);
    if (name.empty()) {
      throw std::invalid_argument(
          "ARTC_EXECUTE_RETRYABLE_STATUSES must be a comma-separated list of status names");
    }
    bool found = false;
    for (const auto& [status_name, status] : kStatusNames) {
      if (name == status_name) {
        statuses.push_back(status);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::invalid_argument("unknown gRPC status in ARTC_EXECUTE_RETRYABLE_STATUSES: " +
                                  std::string(name));
    }
    if (separator == std::string_view::npos) break;
    remaining.remove_prefix(separator + 1);
  }
  return statuses;
}

artc::control::ControllerConfig controller_config_from_environment(double smoothing) {
  artc::control::ControllerConfig config;
  config.latency_ewma_smoothing = smoothing;
  config.aimd.min_limit = environment_integer<std::uint32_t>("ARTC_AIMD_MIN_LIMIT", 1);
  config.aimd.max_limit = environment_integer<std::uint32_t>("ARTC_AIMD_MAX_LIMIT", 512);
  config.aimd.initial_limit = environment_integer<std::uint32_t>("ARTC_AIMD_INITIAL_LIMIT", 64);
  config.aimd.additive_increase = environment_integer<std::uint32_t>("ARTC_AIMD_ALPHA", 2);
  config.aimd.multiplicative_decrease =
      environment_double("ARTC_AIMD_BETA", 0.7);
  config.aimd.control_interval = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_AIMD_INTERVAL_MS", 100));
  config.aimd.minimum_window_samples =
      environment_integer<std::uint64_t>("ARTC_AIMD_MIN_SAMPLES", 16);
  config.aimd.target_latency = std::chrono::microseconds(
      environment_integer<std::int64_t>("ARTC_AIMD_TARGET_LATENCY_US", 50'000));
  config.aimd.overload_error_fraction =
      environment_double("ARTC_AIMD_OVERLOAD_ERROR_FRACTION", 0.1);
  config.deadline_safety_margin = std::chrono::microseconds(
      environment_integer<std::int64_t>("ARTC_DEADLINE_MARGIN_US", 1'000));
  config.default_deadline = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_DEFAULT_DEADLINE_MS", 5'000));
  config.decision_sample_every =
      environment_integer<std::uint32_t>("ARTC_DECISION_SAMPLE_EVERY", 0);
  config.health.minimum_latency_samples =
      environment_integer<std::uint64_t>("ARTC_HEALTH_MIN_SAMPLES", 4);
  config.health.consecutive_failures_to_unavailable =
      environment_integer<std::uint64_t>("ARTC_HEALTH_FAILURES", 3);
  config.health.recovery_successes =
      environment_integer<std::uint64_t>("ARTC_HEALTH_RECOVERY_SUCCESSES", 3);
  config.health.recovery_cooldown = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_HEALTH_RECOVERY_COOLDOWN_MS", 500));
  config.health.degraded_latency_ratio =
      environment_double("ARTC_HEALTH_DEGRADED_RATIO", 2.0);
  config.health.recovered_latency_ratio =
      environment_double("ARTC_HEALTH_RECOVERED_RATIO", 1.5);
  config.recovery_probe_period =
      environment_integer<std::uint32_t>("ARTC_RECOVERY_PROBE_PERIOD", 16);
  return config;
}

artc::rpc::AttemptRuntimeConfig attempt_runtime_config_from_environment() {
  artc::rpc::AttemptRuntimeConfig config;
  config.max_total_attempts =
      environment_integer<std::uint32_t>("ARTC_ATTEMPT_MAX_TOTAL", 3);
  config.max_active_attempts = environment_integer<std::uint32_t>(
      "ARTC_ATTEMPT_MAX_ACTIVE", std::min(config.max_total_attempts, 2U));
  config.jitter_seed = environment_integer<std::uint64_t>("ARTC_ATTEMPT_JITTER_SEED", 1);
  config.minimum_attempt_budget = std::chrono::microseconds(
      environment_integer<std::int64_t>("ARTC_ATTEMPT_MINIMUM_BUDGET_US", 1'000));
  config.hedge_budget.capacity =
      environment_integer<std::uint32_t>("ARTC_HEDGE_BUDGET_CAPACITY", 10);
  config.hedge_budget.refill_per_second =
      environment_double("ARTC_HEDGE_BUDGET_REFILL_PER_SECOND", 1.0);
  config.retry_budget.capacity =
      environment_integer<std::uint32_t>("ARTC_RETRY_BUDGET_CAPACITY", 10);
  config.retry_budget.refill_per_second =
      environment_double("ARTC_RETRY_BUDGET_REFILL_PER_SECOND", 1.0);
  return config;
}

std::unordered_map<std::string, artc::rpc::MethodPolicy>
method_policies_from_environment() {
  artc::rpc::MethodPolicy policy;
  if (const char* value = std::getenv("ARTC_EXECUTE_IDEMPOTENCY")) {
    const std::string_view parsed(value);
    if (parsed == "non_idempotent") {
      policy.idempotency = artc::rpc::Idempotency::kNonIdempotent;
    } else if (parsed == "idempotent") {
      policy.idempotency = artc::rpc::Idempotency::kIdempotent;
    } else {
      throw std::invalid_argument(
          "ARTC_EXECUTE_IDEMPOTENCY must be exactly non_idempotent or idempotent");
    }
  }
  policy.hedging_enabled = environment_bool("ARTC_EXECUTE_HEDGING_ENABLED", false);
  policy.retry_enabled = environment_bool("ARTC_EXECUTE_RETRY_ENABLED", false);
  policy.allow_same_replica_retry =
      environment_bool("ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY", false);
  policy.max_total_attempts =
      environment_integer<std::uint32_t>("ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS", 1);
  policy.max_retries = environment_integer<std::uint32_t>("ARTC_EXECUTE_MAX_RETRIES", 0);
  policy.hedge_delay_min = std::chrono::microseconds(
      environment_integer<std::int64_t>("ARTC_EXECUTE_HEDGE_DELAY_MIN_US", 10'000));
  policy.hedge_delay_max = std::chrono::microseconds(
      environment_integer<std::int64_t>("ARTC_EXECUTE_HEDGE_DELAY_MAX_US", 100'000));
  policy.retry_backoff_base = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS", 10));
  policy.retry_backoff_max = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS", 100));
  policy.retry_jitter_max = std::chrono::milliseconds(
      environment_integer<std::int64_t>("ARTC_EXECUTE_RETRY_JITTER_MAX_MS", 10));
  policy.retryable_statuses = retryable_statuses_from_environment();
  if (policy.allow_same_replica_retry && !policy.retry_enabled) {
    throw std::invalid_argument(
        "ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY requires retries to be enabled");
  }
  artc::rpc::validate(policy);

  std::unordered_map<std::string, artc::rpc::MethodPolicy> result;
  result.emplace("/artc.v1.Traffic/Execute", std::move(policy));
  return result;
}

void print_usage() {
  std::cerr << "usage:\n"
            << "  artc_lab_node service-b <listen> <id> <delay-us>\n"
            << "  artc_lab_node service-a <listen> <id> <delay-us> <service-b-address>\n"
            << "  artc_lab_node router <listen> <policy> <seed> <smoothing> <id=address>...\n"
            << "service-a environment: ARTC_SERVICE_A_UNAVAILABLE_FIRST_N, "
               "ARTC_SERVICE_A_HONOR_CANCELLATION=true|false\n"
            << "router method: ARTC_EXECUTE_IDEMPOTENCY=non_idempotent|idempotent, "
               "ARTC_EXECUTE_HEDGING_ENABLED, ARTC_EXECUTE_RETRY_ENABLED, "
               "ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY, ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS, "
               "ARTC_EXECUTE_MAX_RETRIES, ARTC_EXECUTE_HEDGE_DELAY_MIN_US, "
               "ARTC_EXECUTE_HEDGE_DELAY_MAX_US, ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS, "
               "ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS, ARTC_EXECUTE_RETRY_JITTER_MAX_MS, "
               "ARTC_EXECUTE_RETRYABLE_STATUSES (comma-separated gRPC names)\n"
            << "router runtime: ARTC_ATTEMPT_MAX_TOTAL, ARTC_ATTEMPT_MAX_ACTIVE, "
               "ARTC_ATTEMPT_JITTER_SEED, ARTC_ATTEMPT_MINIMUM_BUDGET_US, "
               "ARTC_HEDGE_BUDGET_CAPACITY, ARTC_HEDGE_BUDGET_REFILL_PER_SECOND, "
               "ARTC_RETRY_BUDGET_CAPACITY, ARTC_RETRY_BUDGET_REFILL_PER_SECOND\n"
            << "all boolean environment values must be exactly true or false; "
               "Execute defaults to non_idempotent with one attempt\n";
}

void print_attempt_summary(const artc::rpc::AttemptSnapshot& snapshot) {
  std::cout << "ARTC_ATTEMPT_SUMMARY"
            << " logical_requests=" << snapshot.logical_requests_total
            << " logical_terminals=" << snapshot.logical_terminal_transitions_total
            << " primary_attempts=" << snapshot.backend_attempts_total[0]
            << " hedge_attempts=" << snapshot.backend_attempts_total[1]
            << " retry_attempts=" << snapshot.backend_attempts_total[2]
            << " primary_wins=" << snapshot.winning_attempts_total[0]
            << " hedge_wins=" << snapshot.winning_attempts_total[1]
            << " retry_wins=" << snapshot.winning_attempts_total[2]
            << " cancelled_attempts=" << snapshot.cancelled_attempts_total
            << " wasted_attempt_time_us=" << snapshot.wasted_attempt_time_us
            << " censored_attempts=" << snapshot.censored_attempts_total
            << " pending_backend_callbacks=" << snapshot.pending_backend_callbacks
            << " hedge_dispatch_errors=" << snapshot.hedge_dispatch_errors_total
            << " attempt_amplification=" << snapshot.attempt_amplification
            << " hedge_budget_available=" << snapshot.hedge_budget.available
            << " hedge_budget_consumed=" << snapshot.hedge_budget.consumed_total
            << " hedge_budget_denied=" << snapshot.hedge_budget.denied_total
            << " hedge_no_target=" << snapshot.hedge_no_target_total
            << " hedge_attempt_limit=" << snapshot.hedge_attempt_limit_total
            << " retry_budget_available=" << snapshot.retry_budget.available
            << " retry_budget_consumed=" << snapshot.retry_budget.consumed_total
            << " retry_budget_denied=" << snapshot.retry_budget.denied_total
            << " retry_no_target=" << snapshot.retry_no_target_total
            << " retry_attempt_limit=" << snapshot.retry_attempt_limit_total
            << " retry_same_replica=" << snapshot.retry_same_replica_total
            << " active_attempts=" << snapshot.active_attempts << '\n';
  static constexpr const char* kinds[]{"primary", "hedge", "retry"};
  std::cout << "# TYPE artc_logical_requests_total counter\n"
            << "artc_logical_requests_total " << snapshot.logical_requests_total << '\n'
            << "# TYPE artc_logical_terminal_transitions_total counter\n"
            << "artc_logical_terminal_transitions_total "
            << snapshot.logical_terminal_transitions_total << '\n'
            << "# TYPE artc_backend_attempts_total counter\n"
            << "# TYPE artc_attempt_completions_total counter\n"
            << "# TYPE artc_winning_attempt_total counter\n";
  for (std::size_t index = 0; index < 3; ++index) {
    std::cout << "artc_backend_attempts_total{kind=\"" << kinds[index] << "\"} "
              << snapshot.backend_attempts_total[index] << '\n'
              << "artc_attempt_completions_total{kind=\"" << kinds[index] << "\"} "
              << snapshot.attempt_completions_total[index] << '\n'
              << "artc_winning_attempt_total{kind=\"" << kinds[index] << "\"} "
              << snapshot.winning_attempts_total[index] << '\n';
  }
  static constexpr const char* cancellation_reasons[]{
      "winner", "deadline", "caller", "shutdown", "internal"};
  std::cout << "# TYPE artc_attempt_cancellations_total counter\n";
  for (std::size_t index = 0; index < std::size(cancellation_reasons); ++index) {
    std::cout << "artc_attempt_cancellations_total{reason=\""
              << cancellation_reasons[index] << "\"} "
              << snapshot.cancellations_by_reason_total[index] << '\n';
  }
  std::cout << "# TYPE artc_hedges_total counter\n"
            << "artc_hedges_total{result=\"started\"} "
            << snapshot.hedge_started_total << '\n'
            << "artc_hedges_total{result=\"budget_denied\"} "
            << snapshot.hedge_budget_denied_total << '\n'
            << "artc_hedges_total{result=\"deadline_denied\"} "
            << snapshot.hedge_deadline_denied_total << '\n'
            << "artc_hedges_total{result=\"overload_denied\"} "
            << snapshot.hedge_overload_denied_total << '\n'
            << "artc_hedges_total{result=\"no_target\"} "
            << snapshot.hedge_no_target_total << '\n'
            << "artc_hedges_total{result=\"attempt_limit\"} "
            << snapshot.hedge_attempt_limit_total << '\n'
            << "# TYPE artc_retries_total counter\n"
            << "artc_retries_total{result=\"started\"} "
            << snapshot.retry_started_total << '\n'
            << "artc_retries_total{result=\"budget_denied\"} "
            << snapshot.retry_budget_denied_total << '\n'
            << "artc_retries_total{result=\"deadline_denied\"} "
            << snapshot.retry_deadline_denied_total << '\n'
            << "artc_retries_total{result=\"not_retryable\"} "
            << snapshot.retry_not_retryable_total << '\n'
            << "artc_retries_total{result=\"no_target\"} "
            << snapshot.retry_no_target_total << '\n'
            << "artc_retries_total{result=\"attempt_limit\"} "
            << snapshot.retry_attempt_limit_total << '\n'
            << "artc_retries_total{result=\"same_replica\"} "
            << snapshot.retry_same_replica_total << '\n'
            << "# TYPE artc_hedge_budget_available gauge\n"
            << "artc_hedge_budget_available " << snapshot.hedge_budget.available << '\n'
            << "# TYPE artc_hedge_budget_consumed_total counter\n"
            << "artc_hedge_budget_consumed_total "
            << snapshot.hedge_budget.consumed_total << '\n'
            << "# TYPE artc_hedge_budget_denied_total counter\n"
            << "artc_hedge_budget_denied_total "
            << snapshot.hedge_budget.denied_total << '\n'
            << "# TYPE artc_retry_budget_available gauge\n"
            << "artc_retry_budget_available " << snapshot.retry_budget.available << '\n'
            << "# TYPE artc_retry_budget_consumed_total counter\n"
            << "artc_retry_budget_consumed_total "
            << snapshot.retry_budget.consumed_total << '\n'
            << "# TYPE artc_retry_budget_denied_total counter\n"
            << "artc_retry_budget_denied_total "
            << snapshot.retry_budget.denied_total << '\n'
            << "# TYPE artc_attempt_amplification gauge\n"
            << "artc_attempt_amplification " << snapshot.attempt_amplification << '\n'
            << "# TYPE artc_wasted_attempt_time_us counter\n"
            << "artc_wasted_attempt_time_us " << snapshot.wasted_attempt_time_us << '\n';
  std::cout << "# TYPE artc_censored_attempts_total counter\n"
            << "artc_censored_attempts_total " << snapshot.censored_attempts_total << '\n';
}

void print_service_work_summary(const artc::rpc::ServiceAWorkSnapshot& snapshot) {
  std::cout << "ARTC_SERVICE_WORK_SUMMARY"
            << " started=" << snapshot.started
            << " completed=" << snapshot.completed
            << " cancellation_signals=" << snapshot.cancellation_signals
            << " completed_after_cancellation="
            << snapshot.completed_after_cancellation
            << " post_cancel_work_time_us=" << snapshot.post_cancel_work_time_us
            << '\n';
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 2) {
      print_usage();
      return 2;
    }
    artc::rpc::block_shutdown_signals();
    const std::string_view role(argv[1]);

    if (role == "service-b" && argc == 5) {
      artc::rpc::ServiceB service(argv[3], parse_integer<std::uint64_t>(argv[4]));
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }
    if (role == "service-a" && argc == 6) {
      artc::rpc::ServiceA service(
          argv[3], parse_integer<std::uint64_t>(argv[4]), argv[5],
          environment_integer<std::uint64_t>("ARTC_SERVICE_A_UNAVAILABLE_FIRST_N", 0),
          environment_bool("ARTC_SERVICE_A_HONOR_CANCELLATION", true));
      auto server = artc::rpc::start_server(argv[2], service);
      const int result = artc::rpc::wait_for_shutdown(*server);
      print_service_work_summary(service.work_snapshot());
      return result;
    }
    if (role == "router" && argc >= 9) {
      validate_router_environment();
      std::vector<artc::rpc::ReplicaConfig> replicas;
      replicas.reserve(static_cast<std::size_t>(argc - 6));
      for (int index = 6; index < argc; ++index) {
        const std::string_view backend(argv[index]);
        const auto separator = backend.find('=');
        if (separator == std::string_view::npos || separator == 0 ||
            separator + 1 == backend.size()) {
          throw std::invalid_argument("backend must use id=address format");
        }
        replicas.push_back({std::string(backend.substr(0, separator)),
                            std::string(backend.substr(separator + 1))});
      }
      artc::rpc::RouterService service(
          std::move(replicas), artc::routing::parse_policy(argv[3]),
          parse_integer<std::uint64_t>(argv[4]), parse_double(argv[5]),
          controller_config_from_environment(parse_double(argv[5])),
          method_policies_from_environment(),
          attempt_runtime_config_from_environment());
      auto server = artc::rpc::start_server(argv[2], service);
      const int result = artc::rpc::wait_for_shutdown(*server, &service);
      print_attempt_summary(service.attempt_snapshot());
      return result;
    }

    print_usage();
    return 2;
  } catch (const std::exception& error) {
    std::cerr << "artc_lab_node: " << error.what() << '\n';
    return 1;
  }
}
