#include "artc/routing/selector.h"

#include <array>
#include <barrier>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace artc::routing {
namespace {

std::vector<std::shared_ptr<ReplicaState>> replicas() {
  return {std::make_shared<ReplicaState>("A1", "a1:5000"),
          std::make_shared<ReplicaState>("A2", "a2:5000"),
          std::make_shared<ReplicaState>("A3", "a3:5000")};
}

TEST(SelectorTest, RoundRobinSequenceAndCounterWrap) {
  auto targets = replicas();
  RoundRobinSelector selector;
  std::vector<std::string> actual;
  for (int i = 0; i < 6; ++i) {
    auto selected = selector.select(targets);
    actual.push_back(selected.replica()->id);
  }
  EXPECT_EQ(actual, (std::vector<std::string>{"A1", "A2", "A3", "A1", "A2", "A3"}));

  RoundRobinSelector wrapping(std::numeric_limits<std::uint64_t>::max() - 1);
  EXPECT_EQ(wrapping.select(targets).replica()->id, "A3");
  EXPECT_EQ(wrapping.select(targets).replica()->id, "A1");
  EXPECT_EQ(wrapping.select(targets).replica()->id, "A2");
}

TEST(SelectorTest, LeastInflightUsesStableTieBreakAndExactLeases) {
  auto targets = replicas();
  LeastInflightSelector selector;
  auto first = selector.select(targets);
  auto second = selector.select(targets);
  auto third = selector.select(targets);
  auto fourth = selector.select(targets);
  EXPECT_EQ(first.replica()->id, "A1");
  EXPECT_EQ(second.replica()->id, "A2");
  EXPECT_EQ(third.replica()->id, "A3");
  EXPECT_EQ(fourth.replica()->id, "A1");
  EXPECT_EQ(targets[0]->inflight.load(), 2U);
  first.release();
  first.release();
  EXPECT_EQ(targets[0]->inflight.load(), 1U);
  fourth = std::move(second);
  EXPECT_EQ(targets[1]->inflight.load(), 1U);
}

TEST(SelectorTest, EmptyAndNullPoolsFailBeforeSelection) {
  RoundRobinSelector selector;
  EXPECT_THROW(static_cast<void>(selector.select({})), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(selector.select({nullptr})), std::invalid_argument);
}

TEST(SelectorTest, EwmaColdStartSamplesEachReplicaBeforeScoring) {
  auto targets = replicas();
  EwmaLatencySelector selector(0.5);
  for (const auto& target : targets) {
    auto lease = selector.select(targets);
    EXPECT_EQ(lease.replica()->id, target->id);
    selector.record_latency(lease.replica(), 10.0);
  }
  auto selected = selector.select(targets);
  EXPECT_EQ(selected.replica()->id, "A1");
  selector.record_latency(selected.replica(), 30.0);
  auto after_update = selector.select(targets);
  EXPECT_EQ(after_update.replica()->id, "A2");
  after_update.release();
  EXPECT_TRUE(targets[0]->observe_latency(10.0, 0.5));
  EXPECT_FALSE(targets[0]->observe_latency(
      std::numeric_limits<double>::quiet_NaN(), 0.5));
  EXPECT_FALSE(targets[0]->observe_latency(
      std::numeric_limits<double>::infinity(), 0.5));
  EXPECT_FALSE(targets[0]->observe_latency(-1.0, 0.5));
  EXPECT_FALSE(targets[0]->observe_latency(1.0, 0.0));
  EXPECT_THROW(EwmaLatencySelector(1.1), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   EwmaLatencySelector(std::numeric_limits<double>::infinity())),
               std::invalid_argument);
}

TEST(SelectorTest, P2CSeedIsRepeatableAndSmallPoolsAreDefined) {
  auto first_pool = replicas();
  auto second_pool = replicas();
  for (std::size_t i = 0; i < first_pool.size(); ++i) {
    first_pool[i]->observe_latency(static_cast<double>((i + 1) * 10), 0.2);
    second_pool[i]->observe_latency(static_cast<double>((i + 1) * 10), 0.2);
  }
  P2CLatencyInflightSelector first(1234, 0.2);
  P2CLatencyInflightSelector second(1234, 0.2);
  for (int i = 0; i < 50; ++i) {
    auto a = first.select(first_pool);
    auto b = second.select(second_pool);
    EXPECT_EQ(a.replica()->id, b.replica()->id);
  }
  auto one = std::vector<std::shared_ptr<ReplicaState>>{first_pool[0]};
  EXPECT_EQ(first.select(one).replica()->id, "A1");
  EXPECT_THROW(P2CLatencyInflightSelector(0, 0.0), std::invalid_argument);
}

TEST(SelectorTest, ConcurrentSelectionsReleaseEveryInflightReservation) {
  auto targets = replicas();
  LeastInflightSelector selector;
  constexpr std::size_t kThreads = 8;
  constexpr std::size_t kSelections = 4000;
  std::barrier gate(static_cast<std::ptrdiff_t>(kThreads));
  std::array<std::thread, kThreads> workers;
  for (std::size_t i = 0; i < kThreads; ++i) {
    workers[i] = std::thread([&] {
      gate.arrive_and_wait();
      for (std::size_t j = 0; j < kSelections; ++j) {
        auto lease = selector.select(targets);
        EXPECT_NE(lease.replica(), nullptr);
      }
    });
  }
  for (auto& worker : workers) worker.join();
  for (const auto& target : targets) EXPECT_EQ(target->inflight.load(), 0U);
}

TEST(SelectorTest, PolicyNamesRejectUnknownValues) {
  EXPECT_EQ(parse_policy("round_robin"), Policy::kRoundRobin);
  EXPECT_EQ(parse_policy("least_inflight"), Policy::kLeastInflight);
  EXPECT_EQ(parse_policy("ewma_latency"), Policy::kEwmaLatency);
  EXPECT_EQ(parse_policy("p2c_latency_inflight"), Policy::kP2CLatencyInflight);
  EXPECT_THROW(static_cast<void>(parse_policy("adaptive")), std::invalid_argument);
}

}  // namespace
}  // namespace artc::routing
