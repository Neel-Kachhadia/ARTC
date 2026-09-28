#include "artc/bench/histogram.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace artc::bench {
namespace {

using namespace std::chrono_literals;

TEST(HistogramTest, EmptySummaryHasNoPercentiles) {
  Histogram histogram;
  const auto summary = histogram.summary();
  EXPECT_EQ(summary.sample_count, 0U);
  EXPECT_FALSE(summary.minimum_us);
  EXPECT_FALSE(summary.maximum_us);
  EXPECT_FALSE(summary.mean_us);
  EXPECT_FALSE(summary.p50_us);
  EXPECT_FALSE(summary.p999_us);
}

TEST(HistogramTest, HdrPercentilesMatchKnownSampleSet) {
  Histogram histogram;
  for (const auto value : {1us, 2us, 3us, 10us, 1000us}) {
    ASSERT_TRUE(histogram.record(value));
  }
  const auto summary = histogram.summary();
  EXPECT_EQ(summary.sample_count, 5U);
  EXPECT_EQ(summary.minimum_us, 1U);
  EXPECT_EQ(summary.maximum_us, 1000U);
  EXPECT_EQ(summary.p50_us, 3U);
  EXPECT_EQ(summary.p95_us, 1000U);
  EXPECT_EQ(summary.p99_us, 1000U);
  EXPECT_FALSE(summary.p999_us);
}

TEST(HistogramTest, NanosecondInputsAndLargeRangesStayBounded) {
  Histogram histogram;
  EXPECT_TRUE(histogram.record(1ns));
  EXPECT_TRUE(histogram.record(1h));
  EXPECT_FALSE(histogram.record(1h + 1us));
  const auto summary = histogram.summary();
  EXPECT_EQ(summary.minimum_us, 1U);
  ASSERT_TRUE(summary.maximum_us);
  EXPECT_GE(*summary.maximum_us, 3'600'000'000U);
  EXPECT_LE(*summary.maximum_us, 3'604'000'000U);
}

TEST(HistogramTest, P999IsOmittedUntilTailHasEnoughSamples) {
  Histogram histogram;
  for (std::uint64_t i = 0; i < kP999MinimumSamples - 1; ++i) {
    ASSERT_TRUE(histogram.record(10us));
  }
  EXPECT_FALSE(histogram.summary().p999_us);
  ASSERT_TRUE(histogram.record(10us));
  EXPECT_EQ(histogram.summary().p999_us, 10U);
}

TEST(HistogramTest, RawBucketsAreAtomicallyWrittenAndRetainCounts) {
  Histogram histogram;
  ASSERT_TRUE(histogram.record(10us));
  ASSERT_TRUE(histogram.record(10us));
  ASSERT_TRUE(histogram.record(20us));
  const auto path = std::filesystem::temp_directory_path() / "artc-histogram-test.hdr";
  histogram.write_raw(path);
  std::ifstream input(path);
  ASSERT_TRUE(input.good());
  std::string contents((std::istreambuf_iterator<char>(input)), {});
  EXPECT_NE(contents.find("format=HdrHistogram_c-bucket-counts-v1"), std::string::npos);
  EXPECT_NE(contents.find("value_us,count"), std::string::npos);
  EXPECT_NE(contents.find("10,2"), std::string::npos);
  EXPECT_NE(contents.find("20,1"), std::string::npos);
  std::filesystem::remove(path);
}

}  // namespace
}  // namespace artc::bench
