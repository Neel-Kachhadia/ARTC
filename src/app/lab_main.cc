#include "artc/rpc/services.h"

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

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
  if (error != std::errc{} || end != value.data() + value.size()) {
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

void print_usage() {
  std::cerr << "usage:\n"
            << "  artc_lab_node service-b <listen> <id> <delay-us>\n"
            << "  artc_lab_node service-a <listen> <id> <delay-us> <service-b-address>\n"
            << "  artc_lab_node router <listen> <policy> <seed> <smoothing> <id=address>...\n";
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
      artc::rpc::ServiceA service(argv[3], parse_integer<std::uint64_t>(argv[4]), argv[5]);
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }
    if (role == "router" && argc >= 9) {
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
          controller_config_from_environment(parse_double(argv[5])));
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }

    print_usage();
    return 2;
  } catch (const std::exception& error) {
    std::cerr << "artc_lab_node: " << error.what() << '\n';
    return 1;
  }
}
