#include "artc/control/phase2.h"

#include <array>
#include <barrier>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace artc::control {
namespace {

using namespace std::chrono_literals;

constexpr SteadyTime kSteady0{std::chrono::seconds(10)};
constexpr SystemTime kSystem0{std::chrono::seconds(100)};

std::vector<std::shared_ptr<routing::ReplicaState>> make_replicas() {
  return {std::make_shared<routing::ReplicaState>("A1", "a1:5001"),
          std::make_shared<routing::ReplicaState>("A2", "a2:5001"),
          std::make_shared<routing::ReplicaState>("A3", "a3:5001")};
}

ControllerConfig test_config() {
  ControllerConfig config;
  config.aimd.min_limit = 1;
  config.aimd.max_limit = 64;
  config.aimd.initial_limit = 10;
  config.aimd.additive_increase = 2;
  config.aimd.control_interval = 100ms;
  config.aimd.minimum_window_samples = 2;
  config.aimd.target_latency = 50ms;
  config.aimd.overload_error_fraction = 0.5;
  config.health.minimum_latency_samples = 2;
  config.health.consecutive_failures_to_unavailable = 3;
  config.health.recovery_successes = 3;
  config.health.recovery_cooldown = 500ms;
  return config;
}

RequestContext request_with_budget(std::chrono::nanoseconds budget) {
  return make_request_context(7, kSystem0 + budget, kSteady0, kSystem0, 5s);
}

TEST(RequestContextTest, PropagatesOnlyRemainingMonotonicBudget) {
  const auto request = request_with_budget(300ms);
  EXPECT_EQ(request.remaining_budget(kSteady0 + 40ms), 260ms);
  const auto downstream = request.downstream_deadline(kSteady0 + 40ms, kSystem0 + 40ms);
  EXPECT_EQ(downstream - (kSystem0 + 40ms), 260ms);
  EXPECT_LE(downstream, request.caller_deadline);
  EXPECT_EQ(request.remaining_budget(kSteady0 + 100ms), 200ms);
}

TEST(RequestContextTest, HandlesExpiredEqualityNanosecondDefaultAndOverflow) {
  const auto exact = request_with_budget(0ns);
  EXPECT_EQ(exact.remaining_budget(kSteady0), 0ns);
  EXPECT_STREQ(admission_result_name(AdmissionResult::kDeadlineExpired),
               "REJECT_DEADLINE_EXPIRED");

  const auto one_ns = request_with_budget(1ns);
  EXPECT_EQ(one_ns.remaining_budget(kSteady0), 1ns);

  const auto missing = make_request_context(9, SystemTime::max(), kSteady0,
                                            kSystem0, 5s);
  EXPECT_EQ(missing.remaining_budget(kSteady0), 5s);
  EXPECT_EQ(missing.downstream_deadline(kSteady0, kSystem0), kSystem0 + 5s);

  const auto near_max = SteadyTime::max() - 1ns;
  const auto overflowing = make_request_context(10, kSystem0 + 5s, near_max,
                                                 kSystem0, 5s);
  EXPECT_EQ(overflowing.effective_deadline, SteadyTime::max());

  const auto distant = make_request_context(
      11, SystemTime::max() - 1ns, kSteady0, SystemTime::min() + 1ns, 5s);
  EXPECT_EQ(distant.effective_deadline, SteadyTime::max());
  EXPECT_GT(distant.downstream_deadline(kSteady0, SystemTime::min() + 1ns),
            SystemTime::min() + 1ns);
  RequestContext wide_budget;
  wide_budget.effective_deadline = SteadyTime::max();
  EXPECT_EQ(wide_budget.remaining_budget(SteadyTime::min()),
            std::chrono::nanoseconds::max());

  auto remaining = request_with_budget(1s).remaining_budget(kSteady0);
  for (int step = 1; step <= 1000; ++step) {
    const auto next = request_with_budget(1s).remaining_budget(kSteady0 + step * 1ms);
    EXPECT_LE(next, remaining);
    remaining = next;
  }
}

TEST(DeadlineFeasibilityTest, IncludesServiceQueueAndMarginWithStrictBoundary) {
  auto request = request_with_budget(21ms);
  ControllerSnapshot snapshot;
  snapshot.route_limit = 4;
  snapshot.route_inflight = 1;
  snapshot.replicas.push_back(ReplicaSnapshot{
      .id = "A1", .health = routing::HealthState::kHealthy,
      .latency_ewma_us = 10'000.0, .latency_p95_us = 10'000.0,
      .latency_samples = 8});

  const auto feasible = evaluate_deadline_feasibility(
      request, kSteady0, snapshot, 10ms, 1ms);
  EXPECT_TRUE(feasible.feasible);
  EXPECT_EQ(feasible.predicted_service_latency, 10ms);
  EXPECT_EQ(feasible.predicted_queue_delay, 0ns);

  request = request_with_budget(20ms);
  EXPECT_FALSE(evaluate_deadline_feasibility(
      request, kSteady0, snapshot, 10ms, 1ms).feasible);

  request = request_with_budget(21ms);
  snapshot.route_inflight = 4;
  const auto queued = evaluate_deadline_feasibility(
      request, kSteady0, snapshot, 10ms, 1ms);
  EXPECT_EQ(queued.predicted_queue_delay, 0ns);
  EXPECT_TRUE(queued.feasible);
}

TEST(DeadlineFeasibilityTest, OneStragglerDoesNotPoisonFastReplicaEstimates) {
  auto request = request_with_budget(100ms);
  ControllerSnapshot snapshot;
  snapshot.replicas = {
      {.id = "A1", .health = routing::HealthState::kHealthy,
       .latency_ewma_us = 50'000.0, .latency_p95_us = 50'000.0},
      {.id = "A2", .health = routing::HealthState::kHealthy,
       .latency_ewma_us = 150'000.0, .latency_p95_us = 150'000.0},
      {.id = "A3", .health = routing::HealthState::kHealthy,
       .latency_ewma_us = 52'000.0, .latency_p95_us = 52'000.0},
  };

  const auto feasible = evaluate_deadline_feasibility(
      request, kSteady0, snapshot, 1ms, 1ms);
  EXPECT_TRUE(feasible.feasible);
  EXPECT_EQ(feasible.predicted_service_latency, 52ms);

  for (auto& replica : snapshot.replicas) {
    replica.latency_ewma_us = 150'000.0;
    replica.latency_p95_us = 150'000.0;
  }
  const auto infeasible = evaluate_deadline_feasibility(
      request, kSteady0, snapshot, 1ms, 1ms);
  EXPECT_FALSE(infeasible.feasible);
  EXPECT_EQ(infeasible.predicted_service_latency, 150ms);
}

TEST(AdmissionGateTest, PermitOwnershipSurvivesMovesLimitChangesAndClose) {
  AdmissionGate gate(1, 8, 2);
  auto first = gate.try_acquire();
  auto second = gate.try_acquire();
  ASSERT_EQ(first.result, AdmissionResult::kAdmitted);
  ASSERT_EQ(second.result, AdmissionResult::kAdmitted);
  EXPECT_EQ(gate.try_acquire().result, AdmissionResult::kConcurrencyLimit);

  gate.set_limit(1);
  EXPECT_EQ(gate.try_acquire().result, AdmissionResult::kConcurrencyLimit);
  first.permit = std::move(second.permit);
  auto state = gate.snapshot();
  EXPECT_EQ(state.inflight, 1U);
  EXPECT_EQ(state.acquired - state.released, state.inflight);
  EXPECT_EQ(gate.try_acquire().result, AdmissionResult::kConcurrencyLimit);

  first.permit.release();
  first.permit.release();
  auto third = gate.try_acquire();
  ASSERT_EQ(third.result, AdmissionResult::kAdmitted);
  gate.close();
  EXPECT_EQ(gate.try_acquire().result, AdmissionResult::kShutdown);
  third.permit.release();
  state = gate.snapshot();
  EXPECT_EQ(state.inflight, 0U);
  EXPECT_EQ(state.acquired, state.released);
  EXPECT_FALSE(state.accepting);
}

TEST(AdmissionGateTest, ConcurrentAdmissionAndReleasePreserveExactAccounting) {
  constexpr std::size_t kThreads = 8;
  AdmissionGate gate(1, 8, 4);
  std::barrier phase(static_cast<std::ptrdiff_t>(kThreads + 1));
  std::array<AdmissionPermit, kThreads> permits;
  std::atomic<std::uint64_t> admitted{0};
  std::array<std::thread, kThreads> threads;
  for (std::size_t i = 0; i < kThreads; ++i) {
    threads[i] = std::thread([&, i] {
      phase.arrive_and_wait();
      auto result = gate.try_acquire();
      if (result.result == AdmissionResult::kAdmitted) {
        admitted.fetch_add(1, std::memory_order_relaxed);
        permits[i] = std::move(result.permit);
      }
      phase.arrive_and_wait();
      phase.arrive_and_wait();
    });
  }
  phase.arrive_and_wait();
  phase.arrive_and_wait();
  auto active = gate.snapshot();
  EXPECT_EQ(admitted.load(), 4U);
  EXPECT_EQ(active.inflight, 4U);
  EXPECT_EQ(active.acquired - active.released, active.inflight);
  phase.arrive_and_wait();
  for (auto& thread : threads) thread.join();
  for (auto& permit : permits) permit.release();
  active = gate.snapshot();
  EXPECT_EQ(active.inflight, 0U);
  EXPECT_EQ(active.acquired, active.released);
}

TEST(ControllerConfigTest, RejectsInvalidLimitsRatesDurationsAndNonFiniteValues) {
  auto config = test_config();
  config.aimd.min_limit = 0;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.max_limit = config.aimd.min_limit - 1;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.multiplicative_decrease = 1.0;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.multiplicative_decrease = std::numeric_limits<double>::quiet_NaN();
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.control_interval = 0ms;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.initial_limit = config.aimd.max_limit + 1;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.multiplicative_decrease = 0.0;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.aimd.overload_error_fraction = std::numeric_limits<double>::infinity();
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.health.degraded_latency_ratio = 1.0;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.health.recovered_latency_ratio = config.health.degraded_latency_ratio;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.latency_ewma_smoothing = std::numeric_limits<double>::infinity();
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.default_deadline = 0ms;
  EXPECT_THROW(validate(config), std::invalid_argument);
  config = test_config();
  config.recovery_probe_period = 0;
  EXPECT_THROW(validate(config), std::invalid_argument);
}

TEST(ReplicaStateTest, BoundsSamplesAndLimitsOutlierImpactOnEwma) {
  routing::ReplicaState replica("A1", "a1:5001");
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(replica.observe_completion(kSteady0 + i * 1ms, 1'000.0,
                                           true, false, false, false, 0.2));
  }
  ASSERT_TRUE(replica.observe_completion(kSteady0 + 20ms, 60'000'000.0,
                                         true, false, false, false, 0.2));
  const auto stats = replica.stats();
  EXPECT_LT(stats.latency_ewma_us, 25'000.0);
  EXPECT_EQ(stats.latency_samples, 11U);
  EXPECT_EQ(stats.latency_p95_us, 60'000'000.0);

  for (int i = 0; i < 200; ++i) {
    ASSERT_TRUE(replica.observe_completion(kSteady0 + 30ms + i * 1ms,
                                           1'000.0, true, false, false,
                                           false, 0.2));
  }
  EXPECT_EQ(replica.stats().latency_p95_us, 1'000.0);
  EXPECT_EQ(replica.stats().latency_samples, 211U);
  EXPECT_FALSE(replica.observe_completion(kSteady0, std::numeric_limits<double>::quiet_NaN(),
                                          true, false, false, false, 0.2));
  EXPECT_TRUE(std::isfinite(replica.stats().latency_ewma_us));
}

TEST(AimdControllerTest, IncreasesOnlyOnControlWindowAndReducesOnOverload) {
  auto config = test_config();
  auto replicas = make_replicas();
  Phase2Controller controller(config, replicas, kSteady0);
  for (int i = 0; i < 2; ++i) {
    controller.record_backend_attempt(0);
    ASSERT_TRUE(controller.record_completion(0, kSteady0 + 20ms,
                                             10'000.0, RequestOutcome::kSuccess));
  }
  controller.tick(kSteady0 + 99ms);
  EXPECT_EQ(controller.admission_snapshot().limit, 10U);
  controller.tick(kSteady0 + 100ms);
  EXPECT_EQ(controller.admission_snapshot().limit, 12U);

  for (int i = 0; i < 2; ++i) {
    controller.record_backend_attempt(1);
    ASSERT_TRUE(controller.record_completion(1, kSteady0 + 120ms,
                                             200'000.0, RequestOutcome::kSuccess));
  }
  controller.tick(kSteady0 + 200ms);
  EXPECT_EQ(controller.admission_snapshot().limit, 8U);
  const auto snapshot = controller.snapshot();
  EXPECT_EQ(snapshot->version, 3U);
  EXPECT_EQ(snapshot->controller_limit_changes, 2U);
  EXPECT_EQ(snapshot->backend_attempts, 4U);
  EXPECT_EQ(snapshot->deadline_goodput, 4U);
  EXPECT_GE(snapshot->route_limit, config.aimd.min_limit);
  EXPECT_LE(snapshot->route_limit, config.aimd.max_limit);
}

TEST(ReplicaHealthTest, RequiresEvidenceAndRecoversAfterCooldown) {
  auto config = test_config();
  config.aimd.minimum_window_samples = 100;
  auto replicas = make_replicas();
  Phase2Controller controller(config, replicas, kSteady0);
  for (int i = 0; i < 3; ++i) {
    controller.record_backend_attempt(1);
    ASSERT_TRUE(controller.record_completion(1, kSteady0 + (i + 1) * 100ms,
                                             10'000.0, RequestOutcome::kFailure));
  }
  controller.tick(kSteady0 + 300ms);
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kUnavailable);
  controller.tick(kSteady0 + 799ms);
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kUnavailable);
  controller.tick(kSteady0 + 899ms);
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kRecovering);

  for (int i = 0; i < 3; ++i) {
    controller.record_backend_attempt(1);
    ASSERT_TRUE(controller.record_completion(1, kSteady0 + 810ms + i * 1ms,
                                             10'000.0, RequestOutcome::kSuccess));
  }
  controller.tick(kSteady0 + 999ms);
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kHealthy);
  EXPECT_GT(controller.snapshot()->version, 1U);
}

TEST(ReplicaHealthTest, SparseSlowProbesMarkDegradedAndRestoreHealthy) {
  auto config = test_config();
  config.health.minimum_latency_samples = 2;
  config.aimd.minimum_window_samples = 100;
  auto replicas = make_replicas();
  Phase2Controller controller(config, replicas, kSteady0);
  const auto observe = [&](std::size_t index, SteadyTime at, double latency_us) {
    controller.record_backend_attempt(index);
    return controller.record_completion(index, at, latency_us, RequestOutcome::kSuccess);
  };

  for (int tick = 1; tick <= 3; ++tick) {
    const auto at = kSteady0 + tick * 100ms;
    ASSERT_TRUE(observe(0, at, 1'000.0));
    ASSERT_TRUE(observe(1, at, 150'000.0));
    ASSERT_TRUE(observe(2, at, 1'100.0));
    controller.tick(at);
    if (tick < 3) {
      EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kHealthy);
    }
  }
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kDegraded);

  for (int tick = 4; tick <= 5; ++tick) {
    const auto at = kSteady0 + tick * 100ms;
    ASSERT_TRUE(observe(0, at, 1'000.0));
    ASSERT_TRUE(observe(1, at, 1'050.0));
    ASSERT_TRUE(observe(2, at, 1'100.0));
    controller.tick(at);
  }
  EXPECT_EQ(replicas[1]->stats().health, routing::HealthState::kHealthy);
}

TEST(AdaptiveSelectorTest, ExcludesUnavailableAndExplainsNormalizedScores) {
  auto config = test_config();
  config.recovery_probe_period = 100;
  auto replicas = make_replicas();
  ControllerSnapshot snapshot;
  snapshot.route_limit = 10;
  snapshot.replicas = {
      ReplicaSnapshot{.id = "A1", .health = routing::HealthState::kHealthy,
                      .latency_ewma_us = 1'000.0, .latency_samples = 4},
      ReplicaSnapshot{.id = "A2", .health = routing::HealthState::kDegraded,
                      .latency_ewma_us = 2'000.0, .error_ewma = 0.5,
                      .latency_samples = 4},
      ReplicaSnapshot{.id = "A3", .health = routing::HealthState::kUnavailable,
                      .latency_ewma_us = 1.0, .latency_samples = 4}};
  AdaptiveSelector selector(config);
  const auto result = selector.select(snapshot, replicas, 1);
  ASSERT_TRUE(result.selected.has_value());
  EXPECT_EQ(*result.selected, 0U);
  EXPECT_EQ(result.eligible, 2U);
  EXPECT_DOUBLE_EQ(result.selected_score.total,
                   result.selected_score.latency_component *
                       result.selected_score.load_component *
                       result.selected_score.error_component *
                       result.selected_score.health_component);
  snapshot.replicas[0].health = routing::HealthState::kUnavailable;
  EXPECT_EQ(selector.select(snapshot, replicas, 2).selected, 1U);
  snapshot.replicas[1].health = routing::HealthState::kUnavailable;
  EXPECT_FALSE(selector.select(snapshot, replicas, 3).selected.has_value());
}

TEST(AdaptiveSelectorTest, DefinesEmptyAndSingleReplicaPools) {
  auto config = test_config();
  AdaptiveSelector selector(config);
  ControllerSnapshot empty_snapshot;
  const std::vector<std::shared_ptr<routing::ReplicaState>> empty_replicas;
  EXPECT_FALSE(selector.select(empty_snapshot, empty_replicas, 1).selected.has_value());

  auto replicas = make_replicas();
  ControllerSnapshot one_snapshot;
  one_snapshot.route_limit = 4;
  one_snapshot.replicas.push_back(ReplicaSnapshot{
      .id = "A1", .health = routing::HealthState::kHealthy,
      .latency_ewma_us = 1'000.0, .latency_samples = 4});
  const std::array<std::shared_ptr<routing::ReplicaState>, 1> one_replica{replicas[0]};
  const auto selected = selector.select(one_snapshot, one_replica, 1);
  ASSERT_TRUE(selected.selected.has_value());
  EXPECT_EQ(*selected.selected, 0U);
  EXPECT_EQ(selected.eligible, 1U);
}

TEST(AdaptiveSelectorTest, PeriodicallyExploresEveryHealthyReplica) {
  auto config = test_config();
  config.recovery_probe_period = 2;
  AdaptiveSelector selector(config);
  auto replicas = make_replicas();
  ControllerSnapshot snapshot;
  snapshot.route_limit = 16;
  snapshot.replicas = {
      ReplicaSnapshot{.id = "A1", .health = routing::HealthState::kHealthy,
                      .latency_ewma_us = 100.0, .latency_samples = 4},
      ReplicaSnapshot{.id = "A2", .health = routing::HealthState::kHealthy,
                      .latency_ewma_us = 1'000.0, .latency_samples = 4},
      ReplicaSnapshot{.id = "A3", .health = routing::HealthState::kHealthy,
                      .latency_ewma_us = 1'200.0, .latency_samples = 4},
  };

  const std::array<std::size_t, 6> expected{0, 0, 1, 0, 2, 0};
  for (std::uint64_t request_id = 0; request_id < expected.size(); ++request_id) {
    const auto result = selector.select(snapshot, replicas, request_id);
    ASSERT_TRUE(result.selected.has_value());
    EXPECT_EQ(*result.selected, expected[request_id]);
  }
}

TEST(ControllerPropertyTest, ReproducibleAdmissionEventsPreservePermitInvariants) {
  constexpr std::uint32_t kSeed = 0x5eed1234;
  std::mt19937 random(kSeed);
  AdmissionGate gate(1, 16, 8);
  std::vector<AdmissionPermit> permits;
  for (std::uint32_t event = 0; event < 20'000; ++event) {
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " event=" << event);
    const auto kind = random() % 4;
    if (kind == 0 && !permits.empty()) {
      permits.back().release();
      permits.pop_back();
    } else if (kind == 1) {
      const auto limit = static_cast<std::uint32_t>(1 + random() % 16);
      gate.set_limit(limit);
    } else {
      auto acquired = gate.try_acquire();
      if (acquired.result == AdmissionResult::kAdmitted) {
        permits.push_back(std::move(acquired.permit));
      }
    }
    const auto snapshot = gate.snapshot();
    EXPECT_EQ(snapshot.acquired - snapshot.released, snapshot.inflight);
    EXPECT_GE(snapshot.limit, 1U);
    EXPECT_LE(snapshot.limit, 16U);
    EXPECT_EQ(snapshot.inflight, permits.size());
  }
  permits.clear();
  const auto final = gate.snapshot();
  EXPECT_EQ(final.inflight, 0U);
  EXPECT_EQ(final.acquired, final.released);
}

TEST(ControllerRaceTest, SnapshotPublicationAndShutdownCanRaceWithTick) {
  auto config = test_config();
  config.aimd.minimum_window_samples = 1;
  auto replicas = make_replicas();
  const auto clock_start = SteadyClock::now();
  Phase2Controller controller(config, replicas, clock_start);
  AdaptiveSelector selector(config);
  controller.start();
  std::barrier start(5);
  std::atomic<bool> failed{false};
  std::thread publisher([&] {
    start.arrive_and_wait();
    for (std::uint64_t tick = 1; tick <= 128; ++tick) {
      controller.tick(clock_start + std::chrono::milliseconds(
                                        static_cast<std::int64_t>(tick * 100)), true);
    }
  });
  std::thread completer([&] {
    start.arrive_and_wait();
    for (std::uint64_t event = 1; event <= 128; ++event) {
      auto acquired = controller.try_acquire();
      if (acquired.result != AdmissionResult::kAdmitted) continue;
      const auto index = static_cast<std::size_t>(event % replicas.size());
      routing::ReplicaLease lease(replicas[index]);
      controller.record_backend_attempt(index);
      const bool failure = index == 1 && event % 3 == 1;
      if (!controller.record_completion(
              index, clock_start + std::chrono::milliseconds(
                                       static_cast<std::int64_t>(event)),
              failure ? 100'000.0 : 5'000.0,
              failure ? RequestOutcome::kFailure : RequestOutcome::kSuccess)) {
        failed.store(true, std::memory_order_relaxed);
      }
      lease.release();
      acquired.permit.release();
    }
  });
  std::thread reader([&] {
    start.arrive_and_wait();
    std::uint64_t previous_version = 0;
    for (std::uint64_t request_id = 1; request_id <= 512; ++request_id) {
      const auto snapshot = controller.snapshot();
      if (snapshot->version < previous_version) failed.store(true, std::memory_order_relaxed);
      previous_version = snapshot->version;
      const auto selected = selector.select(*snapshot, replicas, request_id);
      if (selected.selected &&
          (*selected.selected >= snapshot->replicas.size() ||
           snapshot->replicas[*selected.selected].health == routing::HealthState::kUnavailable)) {
        failed.store(true, std::memory_order_relaxed);
      }
    }
  });
  std::thread stopper([&] {
    start.arrive_and_wait();
    controller.stop();
  });
  start.arrive_and_wait();
  publisher.join();
  completer.join();
  reader.join();
  stopper.join();
  EXPECT_FALSE(failed.load());
  const auto final = controller.admission_snapshot();
  EXPECT_EQ(final.acquired - final.released, final.inflight);
  controller.close_admission();
  EXPECT_EQ(controller.try_acquire().result, AdmissionResult::kShutdown);
}

TEST(ControllerRaceTest, ConcurrentStartAndStopSerializeWorkerLifecycle) {
  auto config = test_config();
  auto replicas = make_replicas();
  Phase2Controller controller(config, replicas, kSteady0);
  constexpr std::size_t kThreads = 8;
  constexpr std::size_t kRounds = 16;
  std::barrier rendezvous(static_cast<std::ptrdiff_t>(kThreads + 1));
  std::array<std::thread, kThreads> threads;

  for (std::size_t i = 0; i < kThreads; ++i) {
    threads[i] = std::thread([&, i] {
      for (std::size_t round = 0; round < kRounds; ++round) {
        rendezvous.arrive_and_wait();
        if (i % 2 == 0) controller.start();
        else controller.stop();
        rendezvous.arrive_and_wait();
      }
    });
  }
  for (std::size_t round = 0; round < kRounds; ++round) {
    rendezvous.arrive_and_wait();
    rendezvous.arrive_and_wait();
  }
  for (auto& thread : threads) thread.join();

  controller.close_admission();
  EXPECT_EQ(controller.try_acquire().result, AdmissionResult::kShutdown);
}

}  // namespace
}  // namespace artc::control
