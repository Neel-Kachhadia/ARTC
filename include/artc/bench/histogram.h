#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

struct hdr_histogram;

namespace artc::bench {

inline constexpr std::uint64_t kP999MinimumSamples = 100'000;

struct HistogramSummary {
  std::uint64_t sample_count{0};
  std::optional<std::uint64_t> minimum_us;
  std::optional<std::uint64_t> maximum_us;
  std::optional<double> mean_us;
  std::optional<std::uint64_t> p50_us;
  std::optional<std::uint64_t> p95_us;
  std::optional<std::uint64_t> p99_us;
  std::optional<std::uint64_t> p999_us;
};

class Histogram {
 public:
  Histogram();
  ~Histogram();
  Histogram(Histogram&& other) noexcept;
  Histogram& operator=(Histogram&& other) noexcept;
  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  [[nodiscard]] bool record(std::chrono::nanoseconds value);
  [[nodiscard]] HistogramSummary summary() const;
  void write_raw(const std::filesystem::path& path) const;

 private:
  struct Deleter {
    void operator()(hdr_histogram* value) const noexcept;
  };
  std::unique_ptr<hdr_histogram, Deleter> histogram_;
};

}  // namespace artc::bench
