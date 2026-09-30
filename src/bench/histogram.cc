#include "artc/bench/histogram.h"

#include <hdr/hdr_histogram.h>

#include <fstream>
#include <stdexcept>
#include <string>
#include <system_error>

namespace artc::bench {
namespace {

constexpr std::int64_t kMaximumLatencyUs = 3'600'000'000;

std::optional<std::uint64_t> percentile(const hdr_histogram* histogram,
                                        double percentile_value) {
  if (histogram->total_count == 0) return std::nullopt;
  return static_cast<std::uint64_t>(
      hdr_value_at_percentile(histogram, percentile_value));
}

}  // namespace

Histogram::Histogram() {
  hdr_histogram* raw = nullptr;
  if (hdr_init(1, kMaximumLatencyUs, 3, &raw) != 0 || raw == nullptr) {
    throw std::runtime_error("HdrHistogram initialization failed");
  }
  histogram_.reset(raw);
}

Histogram::~Histogram() = default;
Histogram::Histogram(Histogram&& other) noexcept = default;
Histogram& Histogram::operator=(Histogram&& other) noexcept = default;

void Histogram::Deleter::operator()(hdr_histogram* value) const noexcept {
  if (value != nullptr) hdr_close(value);
}

bool Histogram::record(std::chrono::nanoseconds value) {
  if (value < std::chrono::nanoseconds::zero()) return false;
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(value).count();
  const std::int64_t recorded = micros == 0 ? 1 : micros;
  if (recorded > kMaximumLatencyUs) return false;
  return hdr_record_value(histogram_.get(), recorded);
}

HistogramSummary Histogram::summary() const {
  HistogramSummary result;
  if (histogram_->total_count <= 0) return result;
  result.sample_count = static_cast<std::uint64_t>(histogram_->total_count);
  result.minimum_us = static_cast<std::uint64_t>(hdr_min(histogram_.get()));
  result.maximum_us = static_cast<std::uint64_t>(hdr_max(histogram_.get()));
  result.mean_us = hdr_mean(histogram_.get());
  result.p50_us = percentile(histogram_.get(), 50.0);
  result.p95_us = percentile(histogram_.get(), 95.0);
  result.p99_us = percentile(histogram_.get(), 99.0);
  if (result.sample_count >= kP999MinimumSamples) {
    result.p999_us = percentile(histogram_.get(), 99.9);
  }
  return result;
}

void Histogram::write_raw(const std::filesystem::path& path) const {
  const auto temporary = path.string() + ".tmp";
  {
    std::ofstream output(temporary, std::ios::out | std::ios::trunc);
    if (!output) throw std::runtime_error("cannot open raw histogram temporary file");
    output << "# format=HdrHistogram_c-bucket-counts-v1\n"
           << "# unit=microseconds\n"
           << "value_us,count\n";
    hdr_iter iterator;
    hdr_iter_recorded_init(&iterator, histogram_.get());
    while (hdr_iter_next(&iterator)) {
      output << iterator.value << ',' << iterator.count << '\n';
    }
    output.flush();
    if (!output) throw std::runtime_error("failed while writing raw histogram");
  }
  std::error_code error;
  std::filesystem::rename(temporary, path, error);
  if (error) {
    std::filesystem::remove(temporary);
    throw std::system_error(error, "atomic raw histogram rename failed");
  }
}

}  // namespace artc::bench
