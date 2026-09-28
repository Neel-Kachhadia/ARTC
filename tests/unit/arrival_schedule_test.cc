#include "artc/bench/arrival_schedule.h"

#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace artc::bench {
namespace {

using namespace std::chrono_literals;

TEST(ArrivalScheduleTest, ConstantRateSchedulesIndependentlyOfCompletion) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kConstant,
                               .duration = 1s,
                               .rate_rps = 10.0,
                               .scripted_arrivals = {}};
  const auto arrivals = make_arrival_schedule(config);
  ASSERT_EQ(arrivals.size(), 10U);
  EXPECT_EQ(arrivals.front(), 0ns);
  EXPECT_EQ(arrivals[1], 100ms);
  EXPECT_EQ(arrivals.back(), 900ms);
}

TEST(ArrivalScheduleTest, PoissonScheduleRepeatsForSameSeed) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kPoisson,
                               .duration = 3s,
                               .rate_rps = 40.0,
                               .seed = 9127,
                               .scripted_arrivals = {}};
  EXPECT_EQ(make_arrival_schedule(config), make_arrival_schedule(config));
}

TEST(ArrivalScheduleTest, StepHasExactRateBoundary) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kStep,
                               .duration = 2s,
                               .rate_rps = 4.0,
                               .initial_rate_rps = 2.0,
                               .scripted_arrivals = {}};
  const auto arrivals = make_arrival_schedule(config);
  ASSERT_EQ(arrivals.size(), 6U);
  EXPECT_EQ(arrivals[0], 0ns);
  EXPECT_EQ(arrivals[1], 500ms);
  EXPECT_EQ(arrivals[2], 1s);
  EXPECT_EQ(arrivals[5], 1750ms);
}

TEST(ArrivalScheduleTest, RampSolvesIntegratedRate) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kRamp,
                               .duration = 2s,
                               .rate_rps = 3.0,
                               .initial_rate_rps = 1.0,
                               .scripted_arrivals = {}};
  const auto arrivals = make_arrival_schedule(config);
  ASSERT_EQ(arrivals.size(), 4U);
  EXPECT_EQ(arrivals.front(), 0ns);
  EXPECT_LT(arrivals.back(), 2s);
  EXPECT_TRUE(std::is_sorted(arrivals.begin(), arrivals.end()));
}

TEST(ArrivalScheduleTest, BurstUsesConfiguredPeriodAndDuration) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kBurst,
                               .duration = 2s,
                               .rate_rps = 1.0,
                               .burst_multiplier = 5.0,
                               .burst_period = 1s,
                               .burst_duration = 100ms,
                               .scripted_arrivals = {}};
  const auto arrivals = make_arrival_schedule(config);
  ASSERT_EQ(arrivals.size(), 4U);
  EXPECT_EQ(arrivals[0], 0ns);
  EXPECT_EQ(arrivals[1], 100ms);
  EXPECT_EQ(arrivals[2], 1s);
  EXPECT_EQ(arrivals[3], 1100ms);
}

TEST(ArrivalScheduleTest, ScriptedArrivalsPreserveEqualTimesAndRejectBadOrder) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kScripted,
                               .duration = 1s,
                               .scripted_arrivals = {0ns, 10ms, 10ms, 999ms}};
  EXPECT_EQ(make_arrival_schedule(config), config.scripted_arrivals);
  config.scripted_arrivals = {20ms, 10ms};
  EXPECT_THROW(static_cast<void>(make_arrival_schedule(config)), std::invalid_argument);
  config.scripted_arrivals = {1s};
  EXPECT_THROW(static_cast<void>(make_arrival_schedule(config)), std::invalid_argument);
}

TEST(ArrivalScheduleTest, InvalidRateDurationAndBoundFailExplicitly) {
  ArrivalScheduleConfig config{.mode = ArrivalMode::kConstant,
                               .duration = 1s,
                               .rate_rps = 0.0,
                               .scripted_arrivals = {}};
  EXPECT_THROW(static_cast<void>(make_arrival_schedule(config)), std::invalid_argument);
  config.rate_rps = 1'000'000'000.0;
  EXPECT_THROW(static_cast<void>(make_arrival_schedule(config)), std::length_error);
  config.duration = 0ns;
  EXPECT_THROW(static_cast<void>(make_arrival_schedule(config)), std::invalid_argument);
}

TEST(ArrivalScheduleTest, ParsingRejectsUnsupportedModes) {
  EXPECT_EQ(parse_arrival_mode("constant"), ArrivalMode::kConstant);
  EXPECT_EQ(parse_arrival_mode("scripted"), ArrivalMode::kScripted);
  EXPECT_THROW(static_cast<void>(parse_arrival_mode("closed_loop")),
               std::invalid_argument);
}

}  // namespace
}  // namespace artc::bench
