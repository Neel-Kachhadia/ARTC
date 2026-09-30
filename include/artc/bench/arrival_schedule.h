#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace artc::bench {

enum class ArrivalMode { kConstant, kPoisson, kStep, kRamp, kBurst, kScripted };

struct ArrivalScheduleConfig {
  ArrivalMode mode{ArrivalMode::kConstant};
  std::chrono::nanoseconds duration{};
  double rate_rps{0.0};
  double initial_rate_rps{0.0};
  double burst_multiplier{5.0};
  std::chrono::nanoseconds burst_period{std::chrono::seconds(1)};
  std::chrono::nanoseconds burst_duration{std::chrono::milliseconds(100)};
  std::uint64_t seed{1};
  std::vector<std::chrono::nanoseconds> scripted_arrivals;
};

inline constexpr std::size_t kMaximumScheduledRequests = 2'000'000;

[[nodiscard]] ArrivalMode parse_arrival_mode(std::string_view value);
[[nodiscard]] std::vector<std::chrono::nanoseconds> make_arrival_schedule(
    const ArrivalScheduleConfig& config);

}  // namespace artc::bench
