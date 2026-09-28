#include "artc/bench/arrival_schedule.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <stdexcept>
#include <string_view>

namespace artc::bench {
namespace {

using Duration = std::chrono::nanoseconds;

long double seconds(Duration duration) {
  return std::chrono::duration<long double>(duration).count();
}

std::size_t event_count(long double expected) {
  if (!std::isfinite(expected) || expected < 0.0L ||
      expected > static_cast<long double>(kMaximumScheduledRequests)) {
    throw std::length_error("arrival schedule exceeds configured request bound");
  }
  if (expected == 0.0L) return 0;
  return static_cast<std::size_t>(std::ceil(expected));
}

void validate_rate(double rate) {
  if (!std::isfinite(rate) || rate <= 0.0) {
    throw std::invalid_argument("arrival rate must be finite and positive");
  }
}

Duration offset_from_seconds(long double value) {
  const long double nanos = value * 1'000'000'000.0L;
  if (!std::isfinite(nanos) || nanos < 0.0L ||
      nanos > static_cast<long double>(Duration::max().count())) {
    throw std::overflow_error("arrival offset is outside nanosecond range");
  }
  return Duration(static_cast<Duration::rep>(nanos));
}

void append_constant(std::vector<Duration>& arrivals, Duration start,
                     Duration length, double rate) {
  validate_rate(rate);
  const std::size_t count = event_count(
      static_cast<long double>(rate) * seconds(length));
  if (count > kMaximumScheduledRequests - arrivals.size()) {
    throw std::length_error("arrival schedule exceeds configured request bound");
  }
  for (std::size_t i = 0; i < count; ++i) {
    const auto offset = offset_from_seconds(static_cast<long double>(i) /
                                            static_cast<long double>(rate));
    if (offset < length) arrivals.push_back(start + offset);
  }
}

void validate_config(const ArrivalScheduleConfig& config) {
  if (config.duration <= Duration::zero()) {
    throw std::invalid_argument("workload duration must be positive");
  }
}

}  // namespace

ArrivalMode parse_arrival_mode(std::string_view value) {
  if (value == "constant") return ArrivalMode::kConstant;
  if (value == "poisson") return ArrivalMode::kPoisson;
  if (value == "step") return ArrivalMode::kStep;
  if (value == "ramp") return ArrivalMode::kRamp;
  if (value == "burst") return ArrivalMode::kBurst;
  if (value == "scripted") return ArrivalMode::kScripted;
  throw std::invalid_argument("unknown arrival mode");
}

std::vector<Duration> make_arrival_schedule(const ArrivalScheduleConfig& config) {
  validate_config(config);
  std::vector<Duration> arrivals;
  const long double duration_s = seconds(config.duration);

  switch (config.mode) {
    case ArrivalMode::kConstant:
      validate_rate(config.rate_rps);
      append_constant(arrivals, Duration::zero(), config.duration, config.rate_rps);
      break;
    case ArrivalMode::kPoisson: {
      validate_rate(config.rate_rps);
      std::mt19937_64 random(config.seed);
      std::exponential_distribution<double> intervals(config.rate_rps);
      long double elapsed = 0.0L;
      while (true) {
        elapsed += static_cast<long double>(intervals(random));
        if (elapsed >= duration_s) break;
        if (arrivals.size() == kMaximumScheduledRequests) {
          throw std::length_error("arrival schedule exceeds configured request bound");
        }
        arrivals.push_back(offset_from_seconds(elapsed));
      }
      break;
    }
    case ArrivalMode::kStep: {
      validate_rate(config.initial_rate_rps);
      validate_rate(config.rate_rps);
      const Duration split = config.duration / 2;
      append_constant(arrivals, Duration::zero(), split, config.initial_rate_rps);
      append_constant(arrivals, split, config.duration - split, config.rate_rps);
      break;
    }
    case ArrivalMode::kRamp: {
      validate_rate(config.initial_rate_rps);
      validate_rate(config.rate_rps);
      const long double start_rate = config.initial_rate_rps;
      const long double rate_delta =
          static_cast<long double>(config.rate_rps) - start_rate;
      const long double total =
          (start_rate + static_cast<long double>(config.rate_rps)) * duration_s / 2.0L;
      const std::size_t count = event_count(total);
      arrivals.reserve(count);
      for (std::size_t i = 0; i < count; ++i) {
        const long double event_number = static_cast<long double>(i);
        const long double a = rate_delta / (2.0L * duration_s);
        const long double discriminant =
            start_rate * start_rate + 4.0L * a * event_number;
        if (discriminant < 0.0L) {
          throw std::domain_error("ramp schedule produced invalid discriminant");
        }
        const long double elapsed =
            (2.0L * event_number) /
            (start_rate + std::sqrt(discriminant));
        if (elapsed < duration_s) arrivals.push_back(offset_from_seconds(elapsed));
      }
      break;
    }
    case ArrivalMode::kBurst: {
      validate_rate(config.rate_rps);
      if (!std::isfinite(config.burst_multiplier) || config.burst_multiplier <= 1.0 ||
          config.burst_period <= Duration::zero() ||
          config.burst_duration <= Duration::zero() ||
          config.burst_duration >= config.burst_period) {
        throw std::invalid_argument("invalid burst period, duration, or multiplier");
      }
      for (Duration start = Duration::zero(); start < config.duration;) {
        const Duration remaining = config.duration - start;
        const Duration burst_end = start + std::min(config.burst_duration, remaining);
        append_constant(arrivals, start, burst_end - start,
                        config.rate_rps * config.burst_multiplier);
        const Duration next = start + std::min(config.burst_period, remaining);
        if (next > burst_end) {
          append_constant(arrivals, burst_end, next - burst_end, config.rate_rps);
        }
        if (next <= start) throw std::overflow_error("burst schedule failed to advance");
        start = next;
      }
      break;
    }
    case ArrivalMode::kScripted:
      if (config.scripted_arrivals.size() > kMaximumScheduledRequests) {
        throw std::length_error("scripted workload exceeds configured request bound");
      }
      if (!std::is_sorted(config.scripted_arrivals.begin(),
                          config.scripted_arrivals.end())) {
        throw std::invalid_argument("scripted arrivals must be sorted");
      }
      for (Duration arrival : config.scripted_arrivals) {
        if (arrival < Duration::zero() || arrival >= config.duration) {
          throw std::invalid_argument("scripted arrival must be inside workload duration");
        }
      }
      arrivals = config.scripted_arrivals;
      break;
  }
  return arrivals;
}

}  // namespace artc::bench
