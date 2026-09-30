#include "artc/control/phase2.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::uint64_t iterations{1'000'000};
  std::filesystem::path output;
};

std::uint64_t parse_iterations(std::string_view value) {
  std::uint64_t result = 0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() ||
      result == 0 || result > 10'000'000) {
    throw std::invalid_argument("--iterations must be in [1, 10000000]");
  }
  return result;
}

Options parse_options(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view key(argv[index]);
    if (key == "--help") {
      std::cout << "artc_control_bench --output DIR [--iterations N]\n";
      std::exit(0);
    }
    if (index + 1 >= argc) throw std::invalid_argument("missing option value");
    const std::string_view value(argv[++index]);
    if (key == "--iterations") options.iterations = parse_iterations(value);
    else if (key == "--output") options.output = value;
    else throw std::invalid_argument("unknown option " + std::string(key));
  }
  if (options.output.empty()) throw std::invalid_argument("--output is required");
  return options;
}

template <typename Operation>
std::vector<std::uint64_t> measure(std::uint64_t iterations, Operation operation,
                                   std::uint64_t* sink) {
  std::vector<std::uint64_t> samples;
  samples.reserve(static_cast<std::size_t>(iterations));
  std::uint64_t result_sink = 0;
  for (std::uint64_t index = 0; index < iterations; ++index) {
    const auto started = Clock::now();
    const std::uint64_t result = operation(index);
    const auto elapsed = Clock::now() - started;
    result_sink ^= result + index;
    samples.push_back(static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
  }
  *sink ^= result_sink;
  return samples;
}

std::uint64_t percentile(const std::vector<std::uint64_t>& sorted, double fraction) {
  const auto index = static_cast<std::size_t>(fraction * static_cast<double>(sorted.size() - 1));
  return sorted[index];
}

void write_summary(std::ostream& output, std::vector<std::uint64_t> samples) {
  std::sort(samples.begin(), samples.end());
  const long double total = std::accumulate(samples.begin(), samples.end(), 0.0L);
  output << "{\"samples\":" << samples.size()
         << ",\"min_ns\":" << samples.front()
         << ",\"mean_ns\":" << static_cast<double>(total / samples.size())
         << ",\"p50_ns\":" << percentile(samples, 0.50)
         << ",\"p95_ns\":" << percentile(samples, 0.95)
         << ",\"p99_ns\":" << percentile(samples, 0.99)
         << ",\"p999_ns\":" << percentile(samples, 0.999)
         << ",\"max_ns\":" << samples.back() << '}';
}

int run(const Options& options) {
  std::filesystem::create_directories(options.output);
  artc::control::ControllerConfig config;
  config.aimd.initial_limit = 64;
  config.aimd.max_limit = 512;
  config.aimd.minimum_window_samples = 16;
  config.health.minimum_latency_samples = 2;
  std::vector<std::shared_ptr<artc::routing::ReplicaState>> replicas{
      std::make_shared<artc::routing::ReplicaState>("A1", "a1:5001"),
      std::make_shared<artc::routing::ReplicaState>("A2", "a2:5001"),
      std::make_shared<artc::routing::ReplicaState>("A3", "a3:5001")};
  artc::control::Phase2Controller controller(config, replicas);
  artc::control::AdaptiveSelector selector(config);
  artc::control::AdmissionGate gate(config.aimd.min_limit, config.aimd.max_limit,
                                    config.aimd.initial_limit);
  std::uint64_t sink = 0;
  auto mutable_snapshot = *controller.snapshot();
  for (std::size_t index = 0; index < mutable_snapshot.replicas.size(); ++index) {
    auto& replica = mutable_snapshot.replicas[index];
    replica.latency_samples = 16;
    replica.latency_ewma_us = 900.0 + static_cast<double>(index) * 100.0;
    replica.latency_p95_us = replica.latency_ewma_us;
  }
  const auto current =
      std::make_shared<const artc::control::ControllerSnapshot>(std::move(mutable_snapshot));

  const auto clock_pair = measure(options.iterations,
      [](std::uint64_t) { return std::uint64_t{0}; }, &sink);
  const auto selection = measure(options.iterations, [&](std::uint64_t index) {
    const auto result = selector.select(*current, replicas, index);
    return result.selected ? static_cast<std::uint64_t>(*result.selected) : 0;
  }, &sink);
  const auto admission = measure(options.iterations, [&](std::uint64_t) {
    auto result = gate.try_acquire();
    const auto admitted = result.result == artc::control::AdmissionResult::kAdmitted;
    result.permit.release();
    return static_cast<std::uint64_t>(admitted);
  }, &sink);
  const auto snapshot_read = measure(options.iterations, [&](std::uint64_t) {
    return controller.snapshot()->version;
  }, &sink);
  auto tick_time = artc::control::SteadyClock::now();
  const auto controller_update = measure(options.iterations, [&](std::uint64_t) {
    tick_time += config.aimd.control_interval;
    controller.tick(tick_time, true);
    return std::uint64_t{1};
  }, &sink);

  std::ofstream manifest(options.output / "manifest.json", std::ios::out | std::ios::trunc);
  if (!manifest) throw std::runtime_error("cannot create microbenchmark manifest");
  manifest << "{\"schema\":\"artc-control-bench-v1\",\"iterations\":"
           << options.iterations << ",\"sink\":" << sink << ",\"clock_pair\":";
  write_summary(manifest, clock_pair);
  manifest << ",\"selector\":";
  write_summary(manifest, selection);
  manifest << ",\"admission\":";
  write_summary(manifest, admission);
  manifest << ",\"snapshot_read\":";
  write_summary(manifest, snapshot_read);
  manifest << ",\"controller_update\":";
  write_summary(manifest, controller_update);
  manifest << "}\n";
  manifest.flush();
  if (!manifest) throw std::runtime_error("failed to write microbenchmark manifest");
  std::cout << "{\"schema\":\"artc-control-bench-v1\",\"iterations\":"
            << options.iterations << ",\"sink\":" << sink << "}\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return run(parse_options(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "artc_control_bench: " << error.what() << '\n';
    return 1;
  }
}
