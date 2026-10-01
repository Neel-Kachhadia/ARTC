#include "artc/rpc/services.h"
#include "attempt_test_driver.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <barrier>
#include <charconv>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

std::atomic<std::uint64_t> held_backend_dispatch_sequence{0};

std::uint64_t attempt_event_seed() {
  const char* value = std::getenv("ARTC_ATTEMPT_EVENT_SEED");
  if (value == nullptr) return 16;
  const std::string_view text(value);
  std::uint64_t seed = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), seed);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("ARTC_ATTEMPT_EVENT_SEED must be an unsigned integer");
  }
  return seed;
}

template <typename Predicate>
bool wait_until(Predicate predicate, std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

template <std::size_t N>
std::array<std::uint64_t, N> seeded_random_values(std::uint64_t seed) {
  std::array<std::uint64_t, N> values{};
  for (auto& value : values) {
    seed += 0x9e37'79b9'7f4a'7c15ULL;
    value = seed;
    value = (value ^ (value >> 30U)) * 0xbf58'476d'1ce4'e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d0'49bb'1331'11ebULL;
    value ^= value >> 31U;
  }
  return values;
}

std::string address_for(int port) {
  return "127.0.0.1:" + std::to_string(port);
}

grpc::Status execute(const std::string& address,
                     const artc::v1::WorkRequest& request,
                     artc::v1::WorkResponse* response,
                     grpc::ClientContext* context) {
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  return stub->Execute(context, request, response);
}

std::vector<std::string> trailing_values(const grpc::ClientContext& context,
                                         std::string_view name) {
  std::vector<std::string> values;
  for (const auto& [key, value] : context.GetServerTrailingMetadata()) {
    if (std::string_view(key.data(), key.size()) == name) {
      values.emplace_back(value.data(), value.size());
    }
  }
  return values;
}

artc::control::ControllerConfig controller_config() {
  artc::control::ControllerConfig config;
  config.aimd.min_limit = 1;
  config.aimd.max_limit = 4;
  config.aimd.initial_limit = 4;
  return config;
}

class HeldBackendCall final {
 public:
  void bind(grpc::ServerUnaryReactor* reactor, artc::v1::WorkResponse* response,
            std::string replica_id, std::uint64_t request_id,
            std::uint64_t dispatch_order) {
    reactor_ = reactor;
    response_ = response;
    replica_id_ = std::move(replica_id);
    request_id_ = request_id;
    dispatch_order_ = dispatch_order;
  }

  bool complete(grpc::Status status = grpc::Status::OK) {
    grpc::ServerUnaryReactor* reactor = nullptr;
    {
      std::lock_guard lock(mutex_);
      if (finished_) return false;
      response_->set_request_id(request_id_);
      response_->set_replica_id(replica_id_);
      response_->set_backend_attempt_count(0);
      finished_ = true;
      reactor = reactor_;
    }
    reactor->Finish(std::move(status));
    return true;
  }

  void cancel() {
    {
      std::lock_guard lock(mutex_);
      cancelled_ = true;
    }
    cancel_changed_.notify_all();
  }

  bool wait_for_cancellation(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return cancel_changed_.wait_for(lock, timeout, [&] { return cancelled_; });
  }

  [[nodiscard]] std::uint64_t dispatch_order() const noexcept {
    return dispatch_order_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cancel_changed_;
  grpc::ServerUnaryReactor* reactor_{nullptr};
  artc::v1::WorkResponse* response_{nullptr};
  std::string replica_id_;
  std::uint64_t request_id_{0};
  std::uint64_t dispatch_order_{0};
  bool finished_{false};
  bool cancelled_{false};
};

class HeldBackendReactor final : public grpc::ServerUnaryReactor {
 public:
  explicit HeldBackendReactor(std::shared_ptr<HeldBackendCall> call)
      : call_(std::move(call)) {}
  void OnCancel() override { call_->cancel(); }
  void OnDone() override { delete this; }

 private:
  std::shared_ptr<HeldBackendCall> call_;
};

class HeldBackend final : public artc::v1::Traffic::CallbackService {
 public:
  explicit HeldBackend(std::string replica_id) : replica_id_(std::move(replica_id)) {}

  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext*,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override {
    auto call = std::make_shared<HeldBackendCall>();
    auto* reactor = new HeldBackendReactor(call);
    call->bind(reactor, response, replica_id_, request->request_id(),
               held_backend_dispatch_sequence.fetch_add(
                   1, std::memory_order_relaxed));
    {
      std::lock_guard lock(mutex_);
      calls_.push_back(call);
    }
    changed_.notify_all();
    return reactor;
  }

  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest*,
                                   artc::v1::HealthResponse* response) override {
    auto* reactor = context->DefaultReactor();
    response->set_ready(true);
    response->set_component_id(replica_id_);
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  bool wait_for_calls(std::size_t count, std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout, [&] { return calls_.size() >= count; });
  }

  std::size_t call_count() {
    std::lock_guard lock(mutex_);
    return calls_.size();
  }

  std::shared_ptr<HeldBackendCall> call(std::size_t index) {
    std::lock_guard lock(mutex_);
    return index < calls_.size() ? calls_[index] : nullptr;
  }

  void complete_all(grpc::Status status) {
    std::vector<std::shared_ptr<HeldBackendCall>> calls;
    {
      std::lock_guard lock(mutex_);
      calls = calls_;
    }
    for (const auto& call : calls) call->complete(status);
  }

 private:
  std::string replica_id_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::vector<std::shared_ptr<HeldBackendCall>> calls_;
};

template <std::size_t N>
std::vector<std::shared_ptr<HeldBackendCall>> calls_since(
    const std::array<HeldBackend*, N>& backends,
    const std::array<std::size_t, N>& first_indices) {
  std::vector<std::shared_ptr<HeldBackendCall>> calls;
  for (std::size_t backend = 0; backend < N; ++backend) {
    for (std::size_t index = first_indices[backend];
         index < backends[backend]->call_count(); ++index) {
      if (auto call = backends[backend]->call(index)) calls.push_back(std::move(call));
    }
  }
  std::sort(calls.begin(), calls.end(), [](const auto& left, const auto& right) {
    return left->dispatch_order() < right->dispatch_order();
  });
  return calls;
}

template <std::size_t N>
bool wait_for_new_calls(const std::array<HeldBackend*, N>& backends,
                        const std::array<std::size_t, N>& first_indices,
                        std::size_t count,
                        std::chrono::milliseconds timeout = 2s) {
  return wait_until(
      [&] { return calls_since(backends, first_indices).size() >= count; },
      timeout);
}

struct RaceResult {
  grpc::Status status;
  artc::rpc::AttemptSnapshot snapshot;
  artc::v1::WorkResponse response;
};

RaceResult run_completion_race(bool race_with_caller_cancel) {
  HeldBackend primary("A1");
  HeldBackend hedge("A2");
  int primary_port = 0;
  int hedge_port = 0;
  auto primary_server = artc::rpc::start_server("127.0.0.1:0", primary,
                                                &primary_port);
  auto hedge_server = artc::rpc::start_server("127.0.0.1:0", hedge,
                                               &hedge_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  artc::rpc::AttemptRuntimeConfig attempt_config;
  attempt_config.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempt_config.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(primary_port)}, {"A2", address_for(hedge_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempt_config);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  request.set_request_id(0);
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = primary.wait_for_calls(1, 2s);
  const bool hedge_entered = hedge.wait_for_calls(1, 2s);
  const auto primary_call = primary.call(0);
  const auto hedge_call = hedge.call(0);
  if (primary_entered && hedge_entered && primary_call && hedge_call) {
    const std::ptrdiff_t participants = race_with_caller_cancel ? 4 : 3;
    std::barrier start(static_cast<std::ptrdiff_t>(participants));
    std::thread primary_completion([&] {
      start.arrive_and_wait();
      primary_call->complete();
    });
    std::thread hedge_completion([&] {
      start.arrive_and_wait();
      hedge_call->complete();
    });
    std::thread cancellation;
    if (race_with_caller_cancel) {
      cancellation = std::thread([&] {
        start.arrive_and_wait();
        context.TryCancel();
      });
    }
    start.arrive_and_wait();
    primary_completion.join();
    hedge_completion.join();
    if (cancellation.joinable()) cancellation.join();
  } else {
    context.TryCancel();
    if (primary_call) primary_call->complete();
    if (hedge_call) hedge_call->complete();
  }
  caller.join();
  const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
  auto snapshot = router.attempt_snapshot();
  while ((snapshot.active_attempts != 0 ||
          snapshot.attempt_completions_total[0] != 1 ||
          snapshot.attempt_completions_total[1] != 1) &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(1ms);
    snapshot = router.attempt_snapshot();
  }
  router.begin_shutdown();
  router_server->Shutdown();
  primary_server->Shutdown();
  hedge_server->Shutdown();
  return {status, snapshot, std::move(response)};
}

TEST(AttemptManagerIntegrationTest, CallbackDrainWaitsThroughBackendOnDone) {
  HeldBackend backend("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend, &backend_port);
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}}, artc::routing::Policy::kRoundRobin,
      17, 0.2, controller_config());
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router, &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  if (!backend.wait_for_calls(1, 1s)) {
    context.TryCancel();
    caller.join();
    FAIL() << "backend did not receive the request";
    return;
  }
  EXPECT_FALSE(router.wait_for_attempt_callbacks(std::chrono::steady_clock::now() + 10ms));
  const auto backend_call = backend.call(0);
  if (!backend_call || !backend_call->complete()) {
    context.TryCancel();
    caller.join();
    FAIL() << "backend call could not be completed";
    return;
  }
  caller.join();
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_TRUE(router.wait_for_attempt_callbacks(std::chrono::steady_clock::now() + 1s));
  EXPECT_EQ(router.attempt_snapshot().pending_backend_callbacks, 0U);

  router.begin_shutdown();
  router_server->Shutdown();
  backend_server->Shutdown();
}

TEST(AttemptManagerIntegrationTest,
     AdmissionPermitStaysHeldUntilLoserAttemptCallbackAccounts) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  std::array<HeldBackend*, 2> backends{&a1, &a2};
  std::array<int, 2> ports{};
  std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }

  auto config = controller_config();
  config.aimd.min_limit = 1;
  config.aimd.max_limit = 1;
  config.aimd.initial_limit = 1;
  config.aimd.control_interval = 10s;
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 5ms;
  method.hedge_delay_max = 5ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config,
      {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  const std::array<std::size_t, 2> first_indices{};
  grpc::ClientContext first_context;
  first_context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest first_request;
  first_request.set_request_id(1);
  artc::v1::WorkResponse first_response;
  grpc::Status first_status;
  std::thread first_caller([&] {
    first_status = execute(address_for(router_port), first_request,
                           &first_response, &first_context);
  });
  const bool primary_entered = wait_for_new_calls(backends, first_indices, 1);
  const auto hedge_timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kHedge);
  if (hedge_timer) driver.deliver(*hedge_timer);
  const bool hedge_entered = wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.backend_attempts_total[1] == 1U;
  });
  const bool hedge_call_entered = wait_for_new_calls(backends, first_indices, 2);
  const auto first_calls = calls_since(backends, first_indices);
  const auto primary_call = first_calls.empty() ? nullptr : first_calls.front();
  const auto hedge_call = first_calls.size() < 2 ? nullptr : first_calls.back();
  driver.pause_next(artc::rpc::testing::Checkpoint::kBackendDoneEntry,
                    artc::rpc::AttemptKind::kPrimary);
  const bool hedge_won = hedge_call && hedge_call->complete();
  if (!hedge_won) first_context.TryCancel();
  first_caller.join();
  const bool loser_cancelled = primary_call &&
      primary_call->wait_for_cancellation(1s);
  const bool loser_callback_paused = driver.wait_until_paused(1s);

  grpc::ClientContext rejected_context;
  rejected_context.set_deadline(std::chrono::system_clock::now() + 1s);
  artc::v1::WorkRequest rejected_request;
  rejected_request.set_request_id(2);
  artc::v1::WorkResponse rejected_response;
  const auto rejected_status = execute(address_for(router_port), rejected_request,
                                       &rejected_response, &rejected_context);
  const auto callback_snapshot = router.attempt_snapshot();
  const bool drain_pending = !router.wait_for_attempt_callbacks(
      std::chrono::steady_clock::now() + 10ms);
  const auto calls_while_loser_paused = calls_since(backends, first_indices);
  driver.release_pause();
  const bool callbacks_drained = router.wait_for_attempt_callbacks(
      std::chrono::steady_clock::now() + 2s);
  if (primary_call) {
    static_cast<void>(primary_call->complete(
        {grpc::StatusCode::CANCELLED, "test cleanup"}));
  }

  grpc::ClientContext recovered_context;
  recovered_context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest recovered_request;
  recovered_request.set_request_id(3);
  artc::v1::WorkResponse recovered_response;
  grpc::Status recovered_status;
  std::thread recovered_caller([&] {
    recovered_status = execute(address_for(router_port), recovered_request,
                               &recovered_response, &recovered_context);
  });
  const bool recovered_attempt_entered = wait_for_new_calls(
      backends, first_indices, 3);
  const auto recovered_calls = calls_since(backends, first_indices);
  if (recovered_attempt_entered && recovered_calls.size() >= 3) {
    static_cast<void>(recovered_calls.back()->complete());
  } else {
    recovered_context.TryCancel();
    for (const auto& call : recovered_calls) {
      static_cast<void>(call->complete(
          {grpc::StatusCode::CANCELLED, "test cleanup"}));
    }
  }
  recovered_caller.join();
  static_cast<void>(router.wait_for_attempt_callbacks(
      std::chrono::steady_clock::now() + 2s));
  const auto final_snapshot = router.attempt_snapshot();

  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  EXPECT_TRUE(primary_entered) << "primary did not reach either held backend";
  EXPECT_TRUE(hedge_timer.has_value()) << "hedge timer was not armed";
  EXPECT_TRUE(hedge_entered) << "hedge timer did not dispatch a secondary";
  EXPECT_TRUE(hedge_call_entered) << "secondary did not reach a held backend";
  EXPECT_TRUE(hedge_won);
  EXPECT_TRUE(first_status.ok()) << first_status.error_message();
  EXPECT_TRUE(loser_cancelled);
  EXPECT_TRUE(loser_callback_paused);
  EXPECT_EQ(rejected_status.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
  EXPECT_EQ(callback_snapshot.pending_backend_callbacks, 1U);
  EXPECT_TRUE(drain_pending);
  EXPECT_EQ(calls_while_loser_paused.size(), 2U);
  EXPECT_TRUE(callbacks_drained);
  EXPECT_TRUE(recovered_attempt_entered);
  EXPECT_TRUE(recovered_status.ok()) << recovered_status.error_message();
  EXPECT_EQ(final_snapshot.active_attempts, 0U);
  EXPECT_EQ(final_snapshot.pending_backend_callbacks, 0U);
}

TEST(AttemptManagerIntegrationTest, HedgeWinsOnDistinctTargetAndCancelsLoser) {
  HeldBackend primary("A1");
  HeldBackend hedge("A2");
  int slow_port = 0;
  int fast_port = 0;
  auto slow_server = artc::rpc::start_server("127.0.0.1:0", primary, &slow_port);
  auto fast_server = artc::rpc::start_server("127.0.0.1:0", hedge, &fast_port);

  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(slow_port)}, {"A2", address_for(fast_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router, &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  request.set_request_id(0);
  request.set_work_units(1);
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = primary.wait_for_calls(1, 2s);
  const bool hedge_entered = hedge.wait_for_calls(1, 2s);
  const auto primary_call = primary.call(0);
  const auto hedge_call = hedge.call(0);
  const bool hedge_released = hedge_call && hedge_call->complete();
  caller.join();
  const bool primary_cancelled =
      primary_call && primary_call->wait_for_cancellation(2s);
  const bool primary_released = primary_call && primary_call->complete(
      {grpc::StatusCode::CANCELLED, "hedge won"});
  const bool drained = wait_until([&] {
    const auto current = router.attempt_snapshot();
    return current.active_attempts == 0 &&
           current.attempt_completions_total[0] == 1 &&
           current.attempt_completions_total[1] == 1 &&
           current.censored_attempts_total == 1;
  });
  const auto snapshot = router.attempt_snapshot();

  EXPECT_TRUE(primary_entered);
  EXPECT_TRUE(hedge_entered);
  EXPECT_TRUE(hedge_released);
  EXPECT_TRUE(primary_cancelled);
  EXPECT_TRUE(primary_released);
  EXPECT_TRUE(drained);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.replica_id(), "A2");
  EXPECT_EQ(response.backend_attempt_count(), 2U);
  EXPECT_EQ(trailing_values(context, "artc-selected-replica"),
            (std::vector<std::string>{"A1"}));
  EXPECT_EQ(trailing_values(context, "artc-winning-attempt-kind"),
            (std::vector<std::string>{"hedge"}));
  EXPECT_EQ(trailing_values(context, "artc-primary-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-hedge-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-retry-attempts"),
            (std::vector<std::string>{"0"}));
  EXPECT_EQ(trailing_values(context, "artc-cancelled-attempts"),
            (std::vector<std::string>{"1"}));

  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[1], 1U);
  EXPECT_EQ(snapshot.cancelled_attempts_total, 1U);
  EXPECT_EQ(snapshot.censored_attempts_total, 1U);
  EXPECT_EQ(snapshot.hedge_budget.consumed_total, 1U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 2.0);
  router.begin_shutdown();
  router_server->Shutdown();
  primary.complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  hedge.complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  slow_server->Shutdown();
  fast_server->Shutdown();
}

TEST(AttemptManagerIntegrationTest, RetryableUnavailableUsesRetryBudgetAndAnotherReplica) {
  artc::rpc::ServiceA transient("A1", 0, {}, 1);
  artc::rpc::ServiceA healthy("A2", 0, {});
  int transient_port = 0;
  int healthy_port = 0;
  auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
                                                 &transient_port);
  auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
                                                &healthy_port);

  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.retry_enabled = true;
  method.max_total_attempts = 2;
  method.max_retries = 1;
  method.retry_backoff_base = 1ms;
  method.retry_backoff_max = 1ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 1, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(transient_port)}, {"A2", address_for(healthy_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router, &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  request.set_request_id(0);
  artc::v1::WorkResponse response;
  const auto status = execute(address_for(router_port), request, &response, &context);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.replica_id(), "A2");
  EXPECT_EQ(response.backend_attempt_count(), 2U);
  EXPECT_EQ(trailing_values(context, "artc-winning-attempt-kind"),
            (std::vector<std::string>{"retry"}));
  EXPECT_EQ(trailing_values(context, "artc-primary-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-hedge-attempts"),
            (std::vector<std::string>{"0"}));
  EXPECT_EQ(trailing_values(context, "artc-retry-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-cancelled-attempts"),
            (std::vector<std::string>{"0"}));
  const auto snapshot = router.attempt_snapshot();
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[2], 1U);
  EXPECT_EQ(snapshot.retry_started_total, 1U);
  EXPECT_EQ(snapshot.retry_budget.consumed_total, 1U);
  EXPECT_EQ(snapshot.hedge_budget.consumed_total, 0U);
  EXPECT_EQ(snapshot.logical_requests_total, 1U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 2.0);
  router.begin_shutdown();
  router_server->Shutdown();
}

TEST(AttemptManagerIntegrationTest, PrimaryAndHedgeFailuresUseOnlyFinalBoundedRetry) {
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  HeldBackend a3("A3");
  std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
  std::array<int, 3> ports{};
  std::array<std::unique_ptr<grpc::Server>, 3> servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    servers[index] = artc::rpc::start_server("127.0.0.1:0", *backends[index],
                                              &ports[index]);
  }

  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.retry_enabled = true;
  method.max_total_attempts = 3;
  method.max_retries = 1;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  method.retry_backoff_base = 1ms;
  method.retry_backoff_max = 1ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.max_total_attempts = 3;
  attempts.max_active_attempts = 2;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 1, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
       {"A3", address_for(ports[2])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const auto initial_deadline = std::chrono::steady_clock::now() + 2s;
  std::vector<std::shared_ptr<HeldBackendCall>> initial_calls;
  std::vector<std::size_t> initial_targets;
  while (std::chrono::steady_clock::now() < initial_deadline &&
         initial_calls.size() < 2) {
    for (std::size_t index = 0; index < backends.size(); ++index) {
      const auto call = backends[index]->call(0);
      if (call && std::find(initial_calls.begin(), initial_calls.end(), call) ==
                      initial_calls.end()) {
        initial_calls.push_back(call);
        initial_targets.push_back(index);
      }
    }
    if (initial_calls.size() < 2) std::this_thread::sleep_for(1ms);
  }
  std::shared_ptr<HeldBackendCall> retry_call;
  std::optional<std::size_t> retry_target;
  if (initial_calls.size() == 2) {
    initial_calls[0]->complete(
        {grpc::StatusCode::UNAVAILABLE, "injected primary/hedge failure"});
    initial_calls[1]->complete(
        {grpc::StatusCode::UNAVAILABLE, "injected primary/hedge failure"});
    const auto retry_deadline = std::chrono::steady_clock::now() + 2s;
    while (!retry_call && std::chrono::steady_clock::now() < retry_deadline) {
      for (std::size_t backend = 0; backend < backends.size(); ++backend) {
        for (std::size_t call_index = 0; call_index < 2; ++call_index) {
          const auto call = backends[backend]->call(call_index);
          if (call && std::find(initial_calls.begin(), initial_calls.end(), call) ==
                          initial_calls.end()) {
            retry_call = call;
            retry_target = backend;
            break;
          }
        }
        if (retry_call) break;
      }
      if (!retry_call) std::this_thread::sleep_for(1ms);
    }
  }
  if (retry_call) retry_call->complete();
  else context.TryCancel();
  caller.join();
  for (auto* backend : backends) {
    backend->complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  }
  const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
  auto snapshot = router.attempt_snapshot();
  while ((snapshot.active_attempts != 0 ||
          snapshot.attempt_completions_total[0] != 1 ||
          snapshot.attempt_completions_total[1] != 1 ||
          snapshot.attempt_completions_total[2] != 1) &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(1ms);
    snapshot = router.attempt_snapshot();
  }
  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : servers) server->Shutdown();

  ASSERT_EQ(initial_calls.size(), 2U);
  EXPECT_NE(initial_targets[0], initial_targets[1]);
  ASSERT_TRUE(retry_call);
  ASSERT_TRUE(retry_target.has_value());
  EXPECT_EQ(std::find(initial_targets.begin(), initial_targets.end(), *retry_target),
            initial_targets.end());
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.backend_attempt_count(), 3U);
  EXPECT_EQ(trailing_values(context, "artc-primary-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-hedge-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(trailing_values(context, "artc-retry-attempts"),
            (std::vector<std::string>{"1"}));
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[2], 1U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 3.0);
  EXPECT_EQ(snapshot.hedge_budget.consumed_total, 1U);
  EXPECT_EQ(snapshot.retry_budget.consumed_total, 1U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest, ZeroHedgeBudgetDeniesSpeculation) {
  artc::rpc::ServiceA a1("A1", 80'000, {});
  artc::rpc::ServiceA a2("A2", 80'000, {});
  int a1_port = 0;
  int a2_port = 0;
  auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &a1_port);
  auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &a2_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(a1_port)}, {"A2", address_for(a2_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  request.set_work_units(1);
  artc::v1::WorkResponse response;
  const auto status = execute(address_for(router_port), request, &response, &context);
  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  a1_server->Shutdown();
  a2_server->Shutdown();

  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.hedge_started_total, 0U);
  EXPECT_EQ(snapshot.hedge_budget_denied_total, 1U);
  EXPECT_EQ(snapshot.hedge_budget.denied_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 1.0);
}

TEST(AttemptManagerIntegrationTest, ZeroRetryBudgetDeniesReplacement) {
  artc::rpc::ServiceA transient("A1", 0, {}, 1);
  artc::rpc::ServiceA healthy("A2", 0, {});
  int transient_port = 0;
  int healthy_port = 0;
  auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
                                                   &transient_port);
  auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
                                                 &healthy_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.retry_enabled = true;
  method.max_total_attempts = 2;
  method.max_retries = 1;
  method.retry_backoff_base = 1ms;
  method.retry_backoff_max = 1ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(transient_port)}, {"A2", address_for(healthy_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  const auto status = execute(address_for(router_port), request, &response, &context);
  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  transient_server->Shutdown();
  healthy_server->Shutdown();

  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
  EXPECT_EQ(snapshot.retry_started_total, 0U);
  EXPECT_EQ(snapshot.retry_budget_denied_total, 1U);
  EXPECT_EQ(snapshot.retry_budget.denied_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 1.0);
}

TEST(AttemptManagerIntegrationTest, HedgeIsSuppressedWhenEveryReplicaIsSlow) {
  artc::rpc::ServiceA a1("A1", 40'000, {});
  artc::rpc::ServiceA a2("A2", 40'000, {});
  int a1_port = 0;
  int a2_port = 0;
  auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &a1_port);
  auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &a2_port);
  auto config = controller_config();
  config.aimd.target_latency = 10ms;
  config.aimd.control_interval = 1ms;
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 200ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 4, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(a1_port)}, {"A2", address_for(a2_port)}},
      artc::routing::Policy::kAdaptiveConcurrencyOnly, 17, 0.2,
      config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  std::array<grpc::Status, 3> statuses;
  for (std::size_t index = 0; index < statuses.size(); ++index) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 2s);
    artc::v1::WorkRequest request;
    request.set_request_id(index + 1);
    request.set_work_units(index == 2 ? 2 : 1);
    artc::v1::WorkResponse response;
    statuses[index] = execute(address_for(router_port), request, &response, &context);
    if (index == 1) {
      EXPECT_TRUE(wait_until([&] {
        const auto snapshot = router.controller_snapshot();
        return snapshot && snapshot->replicas.size() == 2 &&
               std::all_of(snapshot->replicas.begin(), snapshot->replicas.end(),
                           [](const auto& replica) {
                             return replica.latency_samples > 0 &&
                                    replica.latency_p95_us >= 10'000.0;
                           });
      }, 1s));
    }
  }
  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  a1_server->Shutdown();
  a2_server->Shutdown();

  for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(snapshot.backend_attempts_total[0], 3U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.hedge_started_total, 0U);
  EXPECT_EQ(snapshot.hedge_overload_denied_total, 1U);
  EXPECT_EQ(snapshot.attempt_amplification, 1.0);
}

TEST(AttemptManagerIntegrationTest,
     HedgeIsSuppressedWhenOnlyFastReplicaIsRecovering) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  HeldBackend a3("A3");
  std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
  std::array<int, 3> ports{};
  std::array<std::unique_ptr<grpc::Server>, 3> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }

  auto config = controller_config();
  config.aimd.target_latency = 20ms;
  config.aimd.minimum_window_samples = 1000;
  config.aimd.control_interval = 100ms;
  config.health.degraded_latency_ratio = 1000.0;
  config.health.recovery_cooldown = 300ms;
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 5ms;
  method.hedge_delay_max = 5ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 4, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
       {"A3", address_for(ports[2])}},
      artc::routing::Policy::kAdaptiveConcurrencyOnly, 17, 0.2, config,
      {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  auto invoke_and_finish = [&](std::size_t replica,
                               grpc::Status backend_status,
                               std::chrono::milliseconds hold,
                               std::uint64_t request_id) {
    const auto call_index = backends[replica]->call_count();
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 3s);
    artc::v1::WorkRequest request;
    request.set_request_id(request_id);
    artc::v1::WorkResponse response;
    grpc::Status caller_status;
    std::thread caller([&] {
      caller_status = execute(address_for(router_port), request, &response,
                              &context);
    });
    const bool entered = backends[replica]->wait_for_calls(call_index + 1, 1s);
    const auto call = entered ? backends[replica]->call(call_index) : nullptr;
    if (call && hold > 0ms) std::this_thread::sleep_for(hold);
    const bool completed = call && call->complete(backend_status);
    if (!completed) context.TryCancel();
    caller.join();
    const bool drained = wait_until([&] {
      const auto snapshot = router.attempt_snapshot();
      return snapshot.active_attempts == 0 &&
             snapshot.pending_backend_callbacks == 0;
    });
    return entered && completed && drained &&
           (backend_status.ok() ? caller_status.ok()
                                : caller_status.error_code() ==
                                      backend_status.error_code());
  };

  bool warmed = true;
  for (std::size_t index = 0; index < 12 && warmed; ++index) {
    const auto replica = index % backends.size();
    warmed = invoke_and_finish(replica, grpc::Status::OK,
                               replica == 2 ? 0ms : 25ms, index + 1);
  }
  bool fast_history = false;
  bool slow_pair_healthy = false;
  const bool a3_became_unavailable = [&] {
    if (!warmed) return false;
    for (std::size_t index = 0; index < 9; ++index) {
      const auto replica = index % backends.size();
      const auto status = replica == 2
          ? grpc::Status(grpc::StatusCode::UNAVAILABLE, "injected replica failure")
          : grpc::Status::OK;
      if (!invoke_and_finish(replica, status, replica == 2 ? 0ms : 2ms,
                             100 + index)) return false;
    }
    return wait_until([&] {
      const auto snapshot = router.controller_snapshot();
      return snapshot && snapshot->replicas.size() == 3 &&
             snapshot->replicas[2].health == artc::routing::HealthState::kUnavailable;
    });
  }();
  if (a3_became_unavailable) {
    fast_history = [&] {
      const auto snapshot = router.controller_snapshot();
      return snapshot && snapshot->replicas[2].latency_samples >= 4 &&
             snapshot->replicas[2].latency_p95_us < 20'000.0;
    }();
    slow_pair_healthy = [&] {
      const auto snapshot = router.controller_snapshot();
      return snapshot && snapshot->replicas[0].health ==
                             artc::routing::HealthState::kHealthy &&
             snapshot->replicas[1].health ==
                             artc::routing::HealthState::kHealthy &&
             snapshot->replicas[0].latency_p95_us > 20'000.0 &&
             snapshot->replicas[1].latency_p95_us > 20'000.0;
    }();
  }
  const bool a3_recovering = a3_became_unavailable && wait_until([&] {
    const auto snapshot = router.controller_snapshot();
    return snapshot && snapshot->replicas.size() == 3 &&
           snapshot->replicas[2].health == artc::routing::HealthState::kRecovering;
  });

  std::array<std::size_t, 3> first_indices{};
  for (std::size_t index = 0; index < backends.size(); ++index) {
    first_indices[index] = backends[index]->call_count();
  }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  request.set_request_id(999);
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = wait_for_new_calls(backends, first_indices, 1);
  const auto hedge_timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kHedge);
  const auto final_control_snapshot = router.controller_snapshot();
  const bool final_scenario_present =
      final_control_snapshot && final_control_snapshot->replicas.size() == 3 &&
      final_control_snapshot->replicas[0].health ==
          artc::routing::HealthState::kHealthy &&
      final_control_snapshot->replicas[1].health ==
          artc::routing::HealthState::kHealthy &&
      final_control_snapshot->replicas[0].latency_p95_us > 20'000.0 &&
      final_control_snapshot->replicas[1].latency_p95_us > 20'000.0 &&
      final_control_snapshot->replicas[2].health ==
          artc::routing::HealthState::kRecovering &&
      final_control_snapshot->replicas[2].latency_samples >= 4 &&
      final_control_snapshot->replicas[2].latency_p95_us < 20'000.0;
  const auto before = router.attempt_snapshot();
  if (hedge_timer) driver.deliver(*hedge_timer);
  const bool hedge_decided = wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.hedge_overload_denied_total >
               before.hedge_overload_denied_total ||
           snapshot.hedge_started_total > before.hedge_started_total;
  });
  const auto after_decision = router.attempt_snapshot();
  if (after_decision.hedge_started_total > before.hedge_started_total) {
    static_cast<void>(wait_for_new_calls(backends, first_indices, 2));
  }
  const auto final_calls = calls_since(backends, first_indices);
  if (!final_calls.empty()) {
    static_cast<void>(final_calls.front()->complete());
  } else {
    context.TryCancel();
  }
  for (std::size_t index = 1; index < final_calls.size(); ++index) {
    static_cast<void>(final_calls[index]->complete(
        {grpc::StatusCode::CANCELLED, "test cleanup"}));
  }
  caller.join();
  static_cast<void>(wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.active_attempts == 0 &&
       snapshot.pending_backend_callbacks == 0;
  }));
  const auto settled = router.attempt_snapshot();

  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  EXPECT_TRUE(warmed);
  EXPECT_TRUE(a3_became_unavailable);
  EXPECT_TRUE(fast_history);
  EXPECT_TRUE(slow_pair_healthy);
  EXPECT_TRUE(a3_recovering);
  EXPECT_TRUE(primary_entered);
  EXPECT_TRUE(final_scenario_present);
  EXPECT_TRUE(hedge_timer.has_value());
  EXPECT_TRUE(hedge_decided);
  EXPECT_EQ(after_decision.hedge_overload_denied_total,
            before.hedge_overload_denied_total + 1U);
  EXPECT_EQ(after_decision.hedge_started_total, before.hedge_started_total);
  EXPECT_EQ(after_decision.hedge_budget.consumed_total,
            before.hedge_budget.consumed_total);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(settled.backend_attempts_total[1],
            before.backend_attempts_total[1]);
  EXPECT_EQ(settled.active_attempts, 0U);
  EXPECT_EQ(settled.pending_backend_callbacks, 0U);
}

TEST(AttemptManagerIntegrationTest, HedgeOverloadUsesLiveAdmissionPressure) {
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  HeldBackend a3("A3");
  std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
  std::array<int, 3> ports{};
  std::array<std::unique_ptr<grpc::Server>, 3> backend_servers;
  std::array<std::size_t, 3> first_indices{};
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }

  auto config = controller_config();
  config.aimd.initial_limit = 2;
  config.aimd.max_limit = 2;
  config.aimd.control_interval = 10s;
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 250ms;
  method.hedge_delay_max = 250ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 4, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
       {"A3", address_for(ports[2])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config,
      {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  std::array<grpc::ClientContext, 2> contexts;
  std::array<artc::v1::WorkResponse, 2> responses;
  std::array<grpc::Status, 2> statuses;
  std::array<std::thread, 2> callers;
  bool primary_calls_entered = true;
  for (std::size_t index = 0; index < contexts.size(); ++index) {
    contexts[index].set_deadline(std::chrono::system_clock::now() + 3s);
    artc::v1::WorkRequest request;
    request.set_request_id(index + 1);
    callers[index] = std::thread([&, index, request] {
      statuses[index] = execute(address_for(router_port), request,
                                 &responses[index], &contexts[index]);
    });
    if (!wait_for_new_calls(backends, first_indices, index + 1U, 2s)) {
      primary_calls_entered = false;
      break;
    }
  }
  if (!primary_calls_entered) {
    for (auto& context : contexts) context.TryCancel();
    for (const auto& call : calls_since(backends, first_indices)) {
      call->complete({grpc::StatusCode::CANCELLED, "test cleanup"});
    }
  }

  const bool both_denied = wait_until([&] {
    return router.attempt_snapshot().hedge_overload_denied_total == 2;
  }, 3s);
  const auto published = router.controller_snapshot();
  auto calls = calls_since(backends, first_indices);
  for (const auto& call : calls) call->complete();
  for (auto& caller : callers) {
    if (caller.joinable()) caller.join();
  }
  static_cast<void>(wait_until([&] { return router.attempt_snapshot().active_attempts == 0; }));
  const auto snapshot = router.attempt_snapshot();

  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  EXPECT_TRUE(primary_calls_entered);
  EXPECT_TRUE(both_denied);
  ASSERT_NE(published, nullptr);
  EXPECT_EQ(published->route_inflight, 0U)
      << "the published control snapshot intentionally remains stale";
  EXPECT_EQ(calls.size(), 2U);
  EXPECT_EQ(snapshot.hedge_started_total, 0U);
  EXPECT_EQ(snapshot.hedge_overload_denied_total, 2U);
  EXPECT_EQ(snapshot.backend_attempts_total[0], 2U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 2U);
  EXPECT_EQ(snapshot.attempt_amplification, 1.0);
  for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status.error_message();
}

TEST(AttemptManagerIntegrationTest, SecondarySelectionDoesNotAdvancePrimaryRoundRobin) {
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  HeldBackend a3("A3");
  std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
  std::array<int, 3> ports{};
  std::array<std::unique_ptr<grpc::Server>, 3> backend_servers;
  std::array<std::size_t, 3> first_indices{};
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }

  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
       {"A3", address_for(ports[2])}},
      artc::routing::Policy::kAdaptiveConcurrencyOnly, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  std::array<grpc::Status, 2> statuses;
  for (std::size_t index = 0; index < statuses.size(); ++index) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 2s);
    artc::v1::WorkRequest request;
    request.set_request_id(index + 1);
    artc::v1::WorkResponse response;
    std::thread caller([&] {
      statuses[index] = execute(address_for(router_port), request, &response,
                                 &context);
    });
    const bool primary_entered =
        wait_for_new_calls(backends, first_indices, 1U, 2s);
    const bool budget_denied = wait_until([&] {
      return router.attempt_snapshot().hedge_budget_denied_total == index + 1U;
    });
    auto calls = calls_since(backends, first_indices);
    const bool expected_target = calls.size() == 1U &&
        ((index == 0 && a1.call_count() == 1U) ||
         (index == 1 && a2.call_count() == 1U));
    if (!calls.empty()) calls.back()->complete();
    caller.join();
    EXPECT_TRUE(primary_entered);
    EXPECT_TRUE(budget_denied);
    EXPECT_TRUE(expected_target) << "the primary selector sequence must be independent";
    first_indices = {a1.call_count(), a2.call_count(), a3.call_count()};
  }

  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(snapshot.backend_attempts_total[0], 2U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.hedge_budget_denied_total, 2U);
  EXPECT_EQ(snapshot.attempt_amplification, 1.0);
}

TEST(AttemptManagerIntegrationTest, LogicalDeadlineCountsTimeoutAndCallerCancelStaysNeutral) {
  HeldBackend backend("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend,
                                                &backend_port);
  auto config = controller_config();
  config.default_deadline = 100ms;
  config.aimd.control_interval = 1ms;
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::Status deadline_status;
  artc::v1::WorkResponse deadline_response;
  grpc::ClientContext deadline_context;
  artc::v1::WorkRequest deadline_request;
  std::thread deadline_caller([&] {
    deadline_status = execute(address_for(router_port), deadline_request,
                              &deadline_response, &deadline_context);
  });
  const bool deadline_attempt_entered = backend.wait_for_calls(1, 2s);
  const auto deadline_call = backend.call(0);
  const bool deadline_cancelled =
      deadline_call && deadline_call->wait_for_cancellation(2s);
  const bool deadline_call_completed = deadline_call && deadline_call->complete(
      {grpc::StatusCode::CANCELLED, "logical deadline cancelled backend"});
  deadline_caller.join();
  EXPECT_TRUE(wait_until([&] {
    const auto snapshot = router.controller_snapshot();
    return snapshot && snapshot->deadline_misses == 1 &&
           snapshot->replicas.size() == 1 &&
           snapshot->replicas[0].timed_out_total == 1;
  }));

  grpc::Status caller_status;
  artc::v1::WorkResponse caller_response;
  grpc::ClientContext caller_context;
  caller_context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest caller_request;
  std::thread cancelling_caller([&] {
    caller_status = execute(address_for(router_port), caller_request,
                            &caller_response, &caller_context);
  });
  const bool caller_attempt_entered = backend.wait_for_calls(2, 2s);
  const auto caller_call = backend.call(1);
  caller_context.TryCancel();
  const bool caller_cancelled = caller_call && caller_call->wait_for_cancellation(2s);
  const bool caller_call_completed = caller_call && caller_call->complete(
      {grpc::StatusCode::CANCELLED, "caller cancelled backend"});
  cancelling_caller.join();
  static_cast<void>(wait_until([&] {
    return router.attempt_snapshot().active_attempts == 0;
  }));
  const auto attempt_snapshot = router.attempt_snapshot();
  const auto controller_snapshot = router.controller_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  backend_server->Shutdown();

  EXPECT_TRUE(deadline_attempt_entered);
  EXPECT_TRUE(deadline_cancelled);
  EXPECT_TRUE(deadline_call_completed);
  EXPECT_EQ(deadline_status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
  EXPECT_TRUE(caller_attempt_entered);
  EXPECT_TRUE(caller_cancelled);
  EXPECT_TRUE(caller_call_completed);
  EXPECT_EQ(caller_status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(attempt_snapshot.logical_terminal_transitions_total, 2U);
  // The manager alarm and propagated backend deadline share one deadline. The
  // manager may request cancellation first, or gRPC may report the timed-out
  // attempt first; both paths must count one timeout, while this counter only
  // records an explicit manager-issued cancellation.
  EXPECT_LE(attempt_snapshot.cancellations_by_reason_total[
                static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kDeadline)],
            1U);
  EXPECT_EQ(attempt_snapshot.cancellations_by_reason_total[
                static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kCaller)],
            1U);
  ASSERT_NE(controller_snapshot, nullptr);
  EXPECT_EQ(controller_snapshot->deadline_misses, 1U);
  EXPECT_EQ(controller_snapshot->deadline_goodput, 0U);
  ASSERT_EQ(controller_snapshot->replicas.size(), 1U);
  EXPECT_EQ(controller_snapshot->replicas[0].timed_out_total, 1U);
}

TEST(AttemptManagerIntegrationTest, RetryBudgetDenialPreservesDeadlineExceededSemantics) {
  HeldBackend backend("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend,
                                                &backend_port);
  auto config = controller_config();
  config.aimd.control_interval = 1ms;
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.retry_enabled = true;
  method.allow_same_replica_retry = true;
  method.max_total_attempts = 2;
  method.max_retries = 1;
  method.retryable_statuses = {grpc::StatusCode::DEADLINE_EXCEEDED};
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config,
      {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool entered = backend.wait_for_calls(1, 2s);
  const auto call = backend.call(0);
  const bool completed = call && call->complete(
      {grpc::StatusCode::DEADLINE_EXCEEDED, "backend attempt timed out"});
  caller.join();
  EXPECT_TRUE(wait_until([&] {
    return router.attempt_snapshot().retry_budget_denied_total == 1;
  }));
  static_cast<void>(wait_until([&] {
    const auto snapshot = router.controller_snapshot();
    return snapshot && snapshot->deadline_misses == 1 &&
           snapshot->replicas.size() == 1 &&
           snapshot->replicas[0].timed_out_total == 1;
  }));
  const auto attempt_snapshot = router.attempt_snapshot();
  const auto controller_snapshot = router.controller_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  backend_server->Shutdown();

  EXPECT_TRUE(entered);
  EXPECT_TRUE(completed);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
  EXPECT_EQ(attempt_snapshot.retry_budget_denied_total, 1U);
  EXPECT_EQ(attempt_snapshot.retry_started_total, 0U);
  EXPECT_EQ(attempt_snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(attempt_snapshot.backend_attempts_total[2], 0U);
  ASSERT_NE(controller_snapshot, nullptr);
  EXPECT_EQ(controller_snapshot->deadline_misses, 1U);
  EXPECT_EQ(controller_snapshot->deadline_goodput, 0U);
  ASSERT_EQ(controller_snapshot->replicas.size(), 1U);
  EXPECT_EQ(controller_snapshot->replicas[0].timed_out_total, 1U);
}

TEST(AttemptManagerIntegrationTest, DeadlineSuppressesHedgeAndRetry) {
  {
    HeldBackend a1("A1");
    HeldBackend a2("A2");
    int a1_port = 0;
    int a2_port = 0;
    auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &a1_port);
    auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &a2_port);
    auto config = controller_config();
    config.default_deadline = 200ms;
    artc::rpc::MethodPolicy method;
    method.idempotency = artc::rpc::Idempotency::kIdempotent;
    method.hedging_enabled = true;
    method.max_total_attempts = 2;
    method.hedge_delay_min = 250ms;
    method.hedge_delay_max = 250ms;
    artc::rpc::AttemptRuntimeConfig attempts;
    attempts.minimum_attempt_budget = 20ms;
    artc::rpc::RouterService router(
        {{"A1", address_for(a1_port)}, {"A2", address_for(a2_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
        config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
    int router_port = 0;
    auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                  &router_port);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 200ms);
    artc::v1::WorkRequest request;
    artc::v1::WorkResponse response;
    grpc::Status status;
    std::thread caller([&] {
      status = execute(address_for(router_port), request, &response, &context);
    });
    const bool primary_entered = a1.wait_for_calls(1, 2s);
    const bool hedge_denied = wait_until([&] {
      return router.attempt_snapshot().hedge_deadline_denied_total == 1U;
    });
    caller.join();
    a1.complete_all({grpc::StatusCode::CANCELLED, "deadline test cleanup"});
    a2.complete_all({grpc::StatusCode::CANCELLED, "deadline test cleanup"});
    static_cast<void>(wait_until([&] {
      return router.attempt_snapshot().active_attempts == 0U;
    }));
    const auto snapshot = router.attempt_snapshot();
    router.begin_shutdown();
    router_server->Shutdown();
    a1_server->Shutdown();
    a2_server->Shutdown();
    EXPECT_TRUE(primary_entered);
    EXPECT_TRUE(hedge_denied);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
    EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
    EXPECT_EQ(snapshot.hedge_deadline_denied_total, 1U);
  }

  {
    HeldBackend transient("A1");
    HeldBackend healthy("A2");
    int transient_port = 0;
    int healthy_port = 0;
    auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
                                                     &transient_port);
    auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
                                                   &healthy_port);
    artc::rpc::MethodPolicy method;
    method.idempotency = artc::rpc::Idempotency::kIdempotent;
    method.retry_enabled = true;
    method.max_total_attempts = 2;
    method.max_retries = 1;
    method.retry_backoff_base = 500ms;
    method.retry_backoff_max = 500ms;
    method.retry_jitter_max = 0ms;
    auto config = controller_config();
    config.default_deadline = 300ms;
    artc::rpc::RouterService router(
        {{"A1", address_for(transient_port)}, {"A2", address_for(healthy_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
        config, {{"/artc.v1.Traffic/Execute", method}});
    int router_port = 0;
    auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                  &router_port);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 300ms);
    artc::v1::WorkRequest request;
    artc::v1::WorkResponse response;
    grpc::Status status;
    std::thread caller([&] {
      status = execute(address_for(router_port), request, &response, &context);
    });
    const bool primary_entered = transient.wait_for_calls(1, 2s);
    const auto primary_call = transient.call(0);
    const bool primary_failed = primary_call && primary_call->complete(
        {grpc::StatusCode::UNAVAILABLE, "injected transient failure"});
    const bool retry_denied = wait_until([&] {
      return router.attempt_snapshot().retry_deadline_denied_total == 1U;
    });
    caller.join();
    transient.complete_all({grpc::StatusCode::CANCELLED, "retry test cleanup"});
    healthy.complete_all({grpc::StatusCode::CANCELLED, "retry test cleanup"});
    const auto snapshot = router.attempt_snapshot();
    router.begin_shutdown();
    router_server->Shutdown();
    transient_server->Shutdown();
    healthy_server->Shutdown();
    EXPECT_TRUE(primary_entered);
    EXPECT_TRUE(primary_failed);
    EXPECT_TRUE(retry_denied);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
    EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
    EXPECT_EQ(snapshot.retry_deadline_denied_total, 1U);
    EXPECT_EQ(snapshot.retry_budget.consumed_total, 0U);
  }
}

TEST(AttemptManagerIntegrationTest, NonIdempotentUnavailableDoesNotRetryOrHedge) {
  artc::rpc::ServiceA transient("A1", 0, {}, 1);
  artc::rpc::ServiceA healthy("A2", 0, {});
  int transient_port = 0;
  int healthy_port = 0;
  auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
                                                 &transient_port);
  auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
                                                &healthy_port);

  artc::rpc::MethodPolicy method;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 2, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 2, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(transient_port)}, {"A2", address_for(healthy_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router, &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  request.set_request_id(0);
  artc::v1::WorkResponse response;
  const auto status = execute(address_for(router_port), request, &response, &context);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
  const auto snapshot = router.attempt_snapshot();
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
  EXPECT_EQ(snapshot.hedge_budget.consumed_total, 0U);
  EXPECT_EQ(snapshot.retry_budget.consumed_total, 0U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  router.begin_shutdown();
  router_server->Shutdown();
}

TEST(AttemptManagerIntegrationTest, MeasuresCancellationAwareAndIgnoringWork) {
  struct Result {
    grpc::Status status;
    artc::rpc::ServiceAWorkSnapshot work;
    artc::rpc::AttemptSnapshot attempts;
  };
  const auto run = [](bool honor_cancellation) {
    artc::rpc::ServiceB slow_dependency("B-slow", 120'000);
    artc::rpc::ServiceB fast_dependency("B-fast", 0);
    int slow_dependency_port = 0;
    int fast_dependency_port = 0;
    auto slow_dependency_server = artc::rpc::start_server(
        "127.0.0.1:0", slow_dependency, &slow_dependency_port);
    auto fast_dependency_server = artc::rpc::start_server(
        "127.0.0.1:0", fast_dependency, &fast_dependency_port);
    artc::rpc::ServiceA slow("A1", 0, address_for(slow_dependency_port), 0,
                             honor_cancellation);
    artc::rpc::ServiceA fast("A2", 0, address_for(fast_dependency_port));
    int slow_port = 0;
    int fast_port = 0;
    auto slow_server = artc::rpc::start_server("127.0.0.1:0", slow, &slow_port);
    auto fast_server = artc::rpc::start_server("127.0.0.1:0", fast, &fast_port);

    artc::rpc::MethodPolicy method;
    method.idempotency = artc::rpc::Idempotency::kIdempotent;
    method.hedging_enabled = true;
    method.max_total_attempts = 2;
    method.hedge_delay_min = 2ms;
    method.hedge_delay_max = 2ms;
    artc::rpc::AttemptRuntimeConfig attempts;
    attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
    attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
    artc::rpc::RouterService router(
        {{"A1", address_for(slow_port)}, {"A2", address_for(fast_port)}},
        artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
        controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
    int router_port = 0;
    auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                 &router_port);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 2s);
    artc::v1::WorkRequest request;
    request.set_request_id(0);
    request.set_work_units(1);
    request.set_invoke_dependency(true);
    artc::v1::WorkResponse response;
    const auto status = execute(address_for(router_port), request, &response, &context);
    const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
    auto work = slow.work_snapshot();
    auto attempt_snapshot = router.attempt_snapshot();
    while ((work.completed == 0 || attempt_snapshot.active_attempts != 0 ||
            attempt_snapshot.attempt_completions_total[0] != 1 ||
            attempt_snapshot.attempt_completions_total[1] != 1) &&
           std::chrono::steady_clock::now() < drain_deadline) {
      std::this_thread::sleep_for(1ms);
      work = slow.work_snapshot();
      attempt_snapshot = router.attempt_snapshot();
    }
    Result result{status, work, attempt_snapshot};
    router.begin_shutdown();
    router_server->Shutdown();
    slow_server->Shutdown();
    fast_server->Shutdown();
    slow_dependency_server->Shutdown();
    fast_dependency_server->Shutdown();
    return result;
  };

  const auto aware = run(true);
  const auto ignoring = run(false);
  ASSERT_TRUE(aware.status.ok()) << aware.status.error_message();
  ASSERT_TRUE(ignoring.status.ok()) << ignoring.status.error_message();
  ASSERT_EQ(aware.work.cancellation_signals, 1U);
  ASSERT_EQ(ignoring.work.cancellation_signals, 1U);
  EXPECT_EQ(aware.work.completed, 1U);
  EXPECT_EQ(ignoring.work.completed, 1U);
  EXPECT_EQ(aware.attempts.cancelled_attempts_total, 1U);
  EXPECT_EQ(ignoring.attempts.cancelled_attempts_total, 1U);
  EXPECT_LT(aware.work.post_cancel_work_time_us, 25'000U);
  EXPECT_GT(ignoring.work.post_cancel_work_time_us, 50'000U);
  EXPECT_GT(ignoring.work.post_cancel_work_time_us,
            aware.work.post_cancel_work_time_us + 25'000U);
}

TEST(AttemptManagerIntegrationTest, RejectsZeroActiveAttemptCapacity) {
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.max_active_attempts = 0;
  EXPECT_THROW(
      artc::rpc::RouterService(
          {{"A1", "127.0.0.1:1"}}, artc::routing::Policy::kRoundRobin, 1,
          0.2, controller_config(), {}, attempts),
      std::invalid_argument);

  attempts.max_active_attempts = 2;
  attempts.minimum_attempt_budget = std::chrono::microseconds::max();
  EXPECT_THROW(
      artc::rpc::RouterService(
          {{"A1", "127.0.0.1:1"}}, artc::routing::Policy::kRoundRobin, 1,
          0.2, controller_config(), {}, attempts),
               std::invalid_argument);
}

TEST(AttemptManagerIntegrationTest, RejectsMalformedBackendAddresses) {
  for (const std::string_view address : {"", "localhost", ":50051",
                                         "backend:service", "backend:0",
                                         "backend:65536", "bad host:50051",
                                         "bad-.host:50051", "[not-ipv6]:50051"}) {
    EXPECT_THROW(
        artc::rpc::RouterService(
            {{"A1", std::string(address)}}, artc::routing::Policy::kRoundRobin,
            1, 0.2, controller_config(), {}, {}),
        std::invalid_argument)
        << "address=" << address;
  }
  EXPECT_THROW(
      artc::rpc::RouterService({}, artc::routing::Policy::kRoundRobin, 1,
                               0.2, controller_config(), {}, {}),
      std::invalid_argument);
  EXPECT_THROW(
      artc::rpc::RouterService(
          {{"A1", "127.0.0.1:50051"}, {"A1", "127.0.0.1:50052"}},
          artc::routing::Policy::kRoundRobin, 1, 0.2, controller_config(),
          {}, {}),
      std::invalid_argument);
}

TEST(AttemptManagerIntegrationTest, SimultaneousPrimaryAndHedgeSuccessCompletesOnce) {
  const auto result = run_completion_race(false);
  ASSERT_TRUE(result.status.ok()) << result.status.error_message();
  EXPECT_TRUE(result.response.replica_id() == "A1" ||
              result.response.replica_id() == "A2");
  EXPECT_EQ(result.response.backend_attempt_count(), 2U);
  EXPECT_EQ(result.snapshot.logical_requests_total, 1U);
  EXPECT_EQ(result.snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(result.snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(result.snapshot.backend_attempts_total[1], 1U);
  EXPECT_EQ(result.snapshot.winning_attempts_total[0] +
                result.snapshot.winning_attempts_total[1],
            1U);
  EXPECT_EQ(result.snapshot.attempt_completions_total[0], 1U);
  EXPECT_EQ(result.snapshot.attempt_completions_total[1], 1U);
  EXPECT_EQ(result.snapshot.active_attempts, 0U);
  EXPECT_EQ(result.snapshot.attempt_amplification, 2.0);
}

TEST(AttemptManagerIntegrationTest, LateLoserBackendCompletionCannotWinAgain) {
  HeldBackend primary("A1");
  HeldBackend hedge("A2");
  int primary_port = 0;
  int hedge_port = 0;
  auto primary_server = artc::rpc::start_server("127.0.0.1:0", primary,
                                                &primary_port);
  auto hedge_server = artc::rpc::start_server("127.0.0.1:0", hedge,
                                               &hedge_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(primary_port)}, {"A2", address_for(hedge_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = primary.wait_for_calls(1, 2s);
  const bool hedge_entered = hedge.wait_for_calls(1, 2s);
  const auto primary_call = primary.call(0);
  const auto hedge_call = hedge.call(0);
  bool winner_released = false;
  if (primary_entered && hedge_entered && primary_call && hedge_call) {
    winner_released = primary_call->complete();
  } else {
    context.TryCancel();
  }
  caller.join();
  const bool loser_cancelled = hedge_call && hedge_call->wait_for_cancellation(2s);
  const bool loser_released = hedge_call && hedge_call->complete();
  primary.complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  hedge.complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
  auto snapshot = router.attempt_snapshot();
  while ((snapshot.active_attempts != 0 ||
          snapshot.attempt_completions_total[0] != 1 ||
          snapshot.attempt_completions_total[1] != 1) &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(1ms);
    snapshot = router.attempt_snapshot();
  }
  router.begin_shutdown();
  router_server->Shutdown();
  primary_server->Shutdown();
  hedge_server->Shutdown();

  ASSERT_TRUE(primary_entered);
  ASSERT_TRUE(hedge_entered);
  ASSERT_TRUE(winner_released);
  ASSERT_TRUE(loser_cancelled);
  ASSERT_TRUE(loser_released);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.replica_id(), "A1");
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest, UnsolicitedBackendCancelledIsNotCallerCancellation) {
  HeldBackend backend("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend,
                                                &backend_port);
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config());
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool entered = backend.wait_for_calls(1, 2s);
  const auto call = backend.call(0);
  const bool completed = call && call->complete(
      {grpc::StatusCode::CANCELLED, "backend cancelled its own attempt"});
  if (!entered || !completed) context.TryCancel();
  caller.join();
  router.begin_shutdown();
  router_server->Shutdown();
  backend_server->Shutdown();
  ASSERT_TRUE(entered);
  ASSERT_TRUE(completed);

  EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(status.error_message(), "backend cancelled its own attempt");
  const auto snapshot = router.attempt_snapshot();
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.attempt_completions_total[0], 1U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest, CallerCancellationRacesWithBothAttemptCompletions) {
  const auto result = run_completion_race(true);
  EXPECT_TRUE(result.status.ok() ||
              result.status.error_code() == grpc::StatusCode::CANCELLED)
      << result.status.error_message();
  EXPECT_EQ(result.snapshot.logical_requests_total, 1U);
  EXPECT_EQ(result.snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(result.snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(result.snapshot.backend_attempts_total[1], 1U);
  EXPECT_LE(result.snapshot.winning_attempts_total[0] +
                result.snapshot.winning_attempts_total[1],
            1U);
  EXPECT_EQ(result.snapshot.attempt_completions_total[0], 1U);
  EXPECT_EQ(result.snapshot.attempt_completions_total[1], 1U);
  EXPECT_EQ(result.snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest, ShutdownCancelsPendingHedgeTimer) {
  HeldBackend primary("A1");
  HeldBackend hedge("A2");
  int primary_port = 0;
  int hedge_port = 0;
  auto primary_server = artc::rpc::start_server("127.0.0.1:0", primary,
                                                &primary_port);
  auto hedge_server = artc::rpc::start_server("127.0.0.1:0", hedge,
                                               &hedge_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 100ms;
  method.hedge_delay_max = 100ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  artc::rpc::RouterService router(
      {{"A1", address_for(primary_port)}, {"A2", address_for(hedge_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = primary.wait_for_calls(1, 2s);
  const bool hedge_timer_armed = wait_until([&] {
    return router.attempt_snapshot().pending_hedge_timers == 1U;
  });
  router.begin_shutdown();
  caller.join();
  const bool late_hedge = hedge.wait_for_calls(1, 250ms);
  const auto primary_call = primary.call(0);
  if (primary_call) primary_call->complete(
      {grpc::StatusCode::CANCELLED, "router shut down"});
  auto snapshot = router.attempt_snapshot();
  const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
  while (snapshot.active_attempts != 0 &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(1ms);
    snapshot = router.attempt_snapshot();
  }
  router_server->Shutdown();
  primary_server->Shutdown();
  hedge_server->Shutdown();

  ASSERT_TRUE(primary_entered);
  EXPECT_TRUE(hedge_timer_armed);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_FALSE(late_hedge);
  EXPECT_EQ(hedge.call_count(), 0U);
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
}

TEST(AttemptManagerIntegrationTest, ShutdownCancelsPendingRetryTimer) {
  HeldBackend transient("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", transient,
                                                 &backend_port);
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.retry_enabled = true;
  method.allow_same_replica_retry = true;
  method.max_total_attempts = 2;
  method.max_retries = 1;
  method.retry_backoff_base = 500ms;
  method.retry_backoff_max = 500ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}});
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool first_attempt_entered = transient.wait_for_calls(1, 2s);
  const auto first_attempt = transient.call(0);
  const bool first_attempt_failed = first_attempt && first_attempt->complete(
      {grpc::StatusCode::UNAVAILABLE, "transient backend failure"});
  const bool retry_timer_armed = wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.attempt_completions_total[0] == 1U &&
           snapshot.pending_retry_timers == 1U;
  });
  router.begin_shutdown();
  caller.join();
  const bool late_retry = transient.wait_for_calls(2, 600ms);
  const auto snapshot = router.attempt_snapshot();
  router_server->Shutdown();
  backend_server->Shutdown();

  EXPECT_TRUE(first_attempt_entered);
  EXPECT_TRUE(first_attempt_failed);
  EXPECT_TRUE(retry_timer_armed);
  ASSERT_EQ(snapshot.attempt_completions_total[0], 1U);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_FALSE(late_retry);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
  EXPECT_EQ(snapshot.retry_started_total, 0U);
  EXPECT_EQ(snapshot.pending_retry_timers, 0U);
  EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest,
     QueuedHedgeCallbackCannotStartAfterPrimaryCompletes) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  std::array<HeldBackend*, 2> backends{&a1, &a2};
  std::array<int, 2> ports{};
  std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 5ms;
  method.hedge_delay_max = 5ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  const std::array<std::size_t, 2> first_indices{};
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = wait_for_new_calls(backends, first_indices, 1);
  const auto timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kHedge);
  driver.pause_next(artc::rpc::testing::Checkpoint::kHedgeTimerReady);
  std::thread callback;
  if (timer) {
    driver.advance_to(*timer);
    callback = driver.deliver_async(*timer);
  }
  const bool callback_paused = timer && driver.wait_until_paused();
  const auto calls = calls_since(backends, first_indices);
  const auto primary_call = calls.empty() ? nullptr : calls.front();
  const bool primary_won = primary_call && primary_call->complete();
  if (!primary_won) context.TryCancel();
  caller.join();
  driver.release_pause();
  if (callback.joinable()) callback.join();
  for (auto* backend : backends) {
    backend->complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  }
  const bool settled = wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.active_attempts == 0 &&
           snapshot.pending_hedge_timers == 0 &&
           snapshot.pending_backend_callbacks == 0;
  });
  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  EXPECT_TRUE(primary_entered);
  EXPECT_TRUE(callback_paused);
  EXPECT_TRUE(primary_won);
  EXPECT_TRUE(settled);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
  EXPECT_EQ(snapshot.hedge_budget.consumed_total, 0U);
  EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
  EXPECT_EQ(driver.active_timers(), 0U);
}

namespace {

struct DispatchShutdownResult {
  grpc::Status status;
  artc::rpc::AttemptSnapshot snapshot;
  bool primary_entered{false};
  bool timer_armed{false};
  bool callback_paused{false};
  bool hedge_entered_before_shutdown{false};
  bool shutdown_contended{false};
  bool shutdown_closed_fence{false};
  bool drained{false};
  std::size_t backend_call_count{0};
  bool primary_cancelled{false};
  bool hedge_cancelled{false};
};

DispatchShutdownResult run_dispatch_shutdown_race(bool pause_inside_fence) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  std::array<HeldBackend*, 2> backends{&a1, &a2};
  std::array<int, 2> ports{};
  std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.max_total_attempts = 2;
  method.hedge_delay_min = 5ms;
  method.hedge_delay_max = 5ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  DispatchShutdownResult result;
  const std::array<std::size_t, 2> first_indices{};
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  std::thread caller([&] {
    result.status = execute(address_for(router_port), request, &response,
                            &context);
  });
  result.primary_entered = wait_for_new_calls(backends, first_indices, 1);
  const auto timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kHedge);
  result.timer_armed = timer.has_value();
  const auto checkpoint = pause_inside_fence
                              ? artc::rpc::testing::Checkpoint::kInsideDispatchFence
                              : artc::rpc::testing::Checkpoint::kBeforeDispatchFence;
  driver.pause_next(checkpoint, artc::rpc::AttemptKind::kHedge);
  std::thread timer_callback;
  if (timer) {
    driver.advance_to(*timer);
    timer_callback = driver.deliver_async(*timer);
  }
  result.callback_paused = timer && driver.wait_until_paused();

  std::thread shutdown;
  if (result.callback_paused) {
    if (pause_inside_fence) {
      result.hedge_entered_before_shutdown =
          wait_for_new_calls(backends, first_indices, 2);
      shutdown = std::thread([&] { router.begin_shutdown(); });
      result.shutdown_contended = driver.wait_for_checkpoint_count(
          artc::rpc::testing::Checkpoint::kShutdownFenceContended, 1);
    } else {
      shutdown = std::thread([&] { router.begin_shutdown(); });
      result.shutdown_closed_fence = driver.wait_for_checkpoint_count(
          artc::rpc::testing::Checkpoint::kShutdownFenceClosed, 1);
    }
  }
  driver.release_pause();
  if (timer_callback.joinable()) timer_callback.join();
  if (shutdown.joinable()) shutdown.join();
  else router.begin_shutdown();
  if (caller.joinable()) caller.join();

  const auto request_calls = calls_since(backends, first_indices);
  const auto primary_call = request_calls.empty() ? nullptr : request_calls[0];
  const auto hedge_call = request_calls.size() > 1 ? request_calls[1] : nullptr;
  result.backend_call_count = request_calls.size();
  result.shutdown_closed_fence =
      driver.checkpoint_count(
          artc::rpc::testing::Checkpoint::kShutdownFenceClosed) >= 1;
  result.primary_cancelled = primary_call &&
      primary_call->wait_for_cancellation(2s);
  result.hedge_cancelled = hedge_call &&
      hedge_call->wait_for_cancellation(2s);
  for (auto* backend : backends) {
    backend->complete_all({grpc::StatusCode::CANCELLED, "shutdown cleanup"});
  }
  const auto drain_deadline = std::chrono::steady_clock::now() + 2s;
  const bool callbacks_drained = router.wait_for_attempt_callbacks(drain_deadline);
  result.drained = callbacks_drained && wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.active_attempts == 0 &&
           snapshot.pending_backend_callbacks == 0;
  });
  result.snapshot = router.attempt_snapshot();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();
  return result;
}

}  // namespace

TEST(AttemptManagerIntegrationTest,
     ShutdownClosingFenceRejectsQueuedHedgeWithoutConsumingBudget) {
  const auto result = run_dispatch_shutdown_race(false);
  EXPECT_TRUE(result.primary_entered);
  EXPECT_TRUE(result.timer_armed);
  EXPECT_TRUE(result.callback_paused);
  EXPECT_TRUE(result.shutdown_closed_fence);
  EXPECT_EQ(result.backend_call_count, 1U);
  EXPECT_TRUE(result.primary_cancelled);
  EXPECT_TRUE(result.drained);
  EXPECT_EQ(result.status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(result.snapshot.backend_attempts_total[0] +
                result.snapshot.backend_attempts_total[1],
            1U);
  EXPECT_EQ(result.snapshot.hedge_budget.consumed_total, 0U);
  EXPECT_EQ(result.snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(result.snapshot.cancellations_by_reason_total[
                static_cast<std::size_t>(
                    artc::rpc::AttemptCancellationReason::kShutdown)],
            1U);
  EXPECT_EQ(result.snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest,
     ShutdownAfterHedgeCommitCancelsCommittedAttempt) {
  const auto result = run_dispatch_shutdown_race(true);
  EXPECT_TRUE(result.primary_entered);
  EXPECT_TRUE(result.timer_armed);
  EXPECT_TRUE(result.callback_paused);
  EXPECT_TRUE(result.hedge_entered_before_shutdown);
  EXPECT_TRUE(result.shutdown_contended);
  EXPECT_TRUE(result.shutdown_closed_fence);
  EXPECT_EQ(result.backend_call_count, 2U);
  EXPECT_TRUE(result.primary_cancelled);
  EXPECT_TRUE(result.hedge_cancelled);
  EXPECT_TRUE(result.drained);
  EXPECT_EQ(result.status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(result.snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(result.snapshot.backend_attempts_total[1], 1U);
  EXPECT_EQ(result.snapshot.hedge_budget.consumed_total, 1U);
  EXPECT_EQ(result.snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(result.snapshot.cancellations_by_reason_total[
                static_cast<std::size_t>(
                    artc::rpc::AttemptCancellationReason::kShutdown)],
            2U);
  EXPECT_EQ(result.snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest,
     QueuedRetryCallbackCannotStartAfterCallerCancellation) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  std::array<HeldBackend*, 2> backends{&a1, &a2};
  std::array<int, 2> ports{};
  std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.retry_enabled = true;
  method.max_total_attempts = 2;
  method.max_retries = 1;
  method.retry_backoff_base = 5ms;
  method.retry_backoff_max = 5ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 1, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const std::array<std::size_t, 2> first_indices{};
  const bool primary_entered =
      wait_for_new_calls(backends, first_indices, 1, 2s);
  const auto calls = calls_since(backends, first_indices);
  const auto primary_call = calls.empty() ? nullptr : calls.front();
  const bool retryable_failure = primary_call && primary_call->complete(
      {grpc::StatusCode::UNAVAILABLE, "seeded transient failure"});
  const auto retry_timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kRetry);
  driver.pause_next(artc::rpc::testing::Checkpoint::kRetryTimerReady);
  std::thread timer_callback;
  if (retry_timer) {
    driver.advance_to(*retry_timer);
    timer_callback = driver.deliver_async(*retry_timer);
  }
  const bool callback_paused = retry_timer && driver.wait_until_paused();
  context.TryCancel();
  caller.join();
  const bool terminal_before_release = wait_until([&] {
    return router.attempt_snapshot().logical_terminal_transitions_total == 1U;
  });
  driver.release_pause();
  if (timer_callback.joinable()) timer_callback.join();
  const bool drained = wait_until([&] {
    const auto current = router.attempt_snapshot();
    return current.active_attempts == 0 &&
           current.pending_backend_callbacks == 0 &&
           current.pending_retry_timers == 0;
  });
  const auto snapshot = router.attempt_snapshot();
  const auto backend_call_count = a1.call_count() + a2.call_count();
  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();

  EXPECT_TRUE(primary_entered);
  EXPECT_TRUE(retryable_failure);
  EXPECT_TRUE(retry_timer.has_value());
  EXPECT_TRUE(callback_paused);
  EXPECT_TRUE(terminal_before_release);
  EXPECT_TRUE(drained);
  EXPECT_EQ(backend_call_count, 1U);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
  EXPECT_EQ(snapshot.retry_budget.consumed_total, 0U);
  EXPECT_EQ(snapshot.pending_retry_timers, 0U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
}

TEST(AttemptManagerIntegrationTest,
     DeadlinePreventsQueuedAndDispatchingRetry) {
  using artc::rpc::testing::Checkpoint;

  for (const auto checkpoint : {Checkpoint::kRetryTimerReady,
                                Checkpoint::kBeforeDispatchFence}) {
    const bool deadline_wins_while_retry_queued =
        checkpoint == Checkpoint::kRetryTimerReady;
    artc::rpc::testing::AttemptTestDriver driver;
    HeldBackend a1("A1");
    HeldBackend a2("A2");
    std::array<HeldBackend*, 2> backends{&a1, &a2};
    std::array<int, 2> ports{};
    std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
    for (std::size_t index = 0; index < backends.size(); ++index) {
      backend_servers[index] = artc::rpc::start_server(
          "127.0.0.1:0", *backends[index], &ports[index]);
    }

    artc::rpc::MethodPolicy method;
    method.idempotency = artc::rpc::Idempotency::kIdempotent;
    method.retry_enabled = true;
    method.max_total_attempts = 2;
    method.max_retries = 1;
    method.retry_backoff_base = 5ms;
    method.retry_backoff_max = 5ms;
    method.retry_jitter_max = 0ms;
    artc::rpc::AttemptRuntimeConfig attempts;
    attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
    attempts.retry_budget = {.capacity = 1, .refill_per_second = 0.0};
    attempts.test_control = driver.control();
    auto config = controller_config();
    config.default_deadline = 1s;
    config.aimd.control_interval = 1ms;
    artc::rpc::RouterService router(
        {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
        artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config,
        {{"/artc.v1.Traffic/Execute", method}}, attempts);
    int router_port = 0;
    auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                  &router_port);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 3s);
    artc::v1::WorkRequest request;
    artc::v1::WorkResponse response;
    grpc::Status status;
    std::thread caller([&] {
      status = execute(address_for(router_port), request, &response, &context);
    });
    const std::array<std::size_t, 2> first_indices{};
    const bool primary_entered =
        wait_for_new_calls(backends, first_indices, 1, 2s);
    const auto calls = calls_since(backends, first_indices);
    const auto primary_call = calls.empty() ? nullptr : calls.front();
    const auto deadline_timer = driver.wait_for_timer(
        artc::rpc::testing::TimerKind::kDeadline);
    const bool retryable_failure = primary_call && primary_call->complete(
        {grpc::StatusCode::UNAVAILABLE, "seeded transient failure"});
    const auto retry_timer = driver.wait_for_timer(
        artc::rpc::testing::TimerKind::kRetry);
    const bool retry_pending = wait_until([&] {
      return router.attempt_snapshot().pending_retry_timers == 1U;
    });

    std::thread retry_callback;
    std::thread deadline_callback;
    bool retry_paused = false;
    bool deadline_ready = false;
    if (deadline_timer && retry_timer && retry_pending) {
      driver.pause_next(checkpoint, artc::rpc::AttemptKind::kRetry);
      driver.advance_to(*retry_timer);
      retry_callback = driver.deliver_async(*retry_timer);
      retry_paused = driver.wait_until_paused();
      if (retry_paused) {
        driver.advance_to(*deadline_timer);
        if (deadline_wins_while_retry_queued) {
          driver.deliver(*deadline_timer);
          deadline_ready = true;
        } else {
          deadline_callback = driver.deliver_async(*deadline_timer);
          deadline_ready = driver.wait_for_checkpoint_count(
              Checkpoint::kDeadlineTimerReady, 1U);
        }
      }
    }

    const bool terminal_before_retry_release =
        deadline_wins_while_retry_queued && retry_paused && deadline_ready &&
        wait_until([&] {
          return router.attempt_snapshot().logical_terminal_transitions_total ==
                 1U;
        });
    if (!retry_paused || !deadline_ready) context.TryCancel();
    driver.release_pause();
    if (retry_callback.joinable()) retry_callback.join();
    if (deadline_callback.joinable()) deadline_callback.join();
    if (caller.joinable()) caller.join();

    for (auto* backend : backends) {
      backend->complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
    }
    const bool callbacks_drained = router.wait_for_attempt_callbacks(
        std::chrono::steady_clock::now() + 2s);
    const bool drained = callbacks_drained && wait_until([&] {
      const auto snapshot = router.attempt_snapshot();
      return snapshot.active_attempts == 0U &&
             snapshot.pending_backend_callbacks == 0U &&
             snapshot.pending_hedge_timers == 0U &&
             snapshot.pending_retry_timers == 0U;
    });
    const auto snapshot = router.attempt_snapshot();
    const bool controller_accounted = wait_until([&] {
      const auto current = router.controller_snapshot();
      return current && current->deadline_misses == 1U &&
             current->permits_acquired == 1U &&
             current->permits_released == 1U;
    });
    const auto controller = router.controller_snapshot();
    const auto backend_call_count = a1.call_count() + a2.call_count();
    const auto active_timers = driver.active_timers();
    router.begin_shutdown();
    router_server->Shutdown();
    for (auto& server : backend_servers) server->Shutdown();

    EXPECT_TRUE(primary_entered);
    EXPECT_TRUE(deadline_timer.has_value());
    EXPECT_TRUE(retryable_failure);
    EXPECT_TRUE(retry_timer.has_value());
    EXPECT_TRUE(retry_pending);
    EXPECT_TRUE(retry_paused);
    EXPECT_TRUE(deadline_ready);
    if (deadline_wins_while_retry_queued) {
      EXPECT_TRUE(terminal_before_retry_release);
    }
    EXPECT_TRUE(drained);
    EXPECT_TRUE(controller_accounted);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
    EXPECT_EQ(backend_call_count, 1U);
    EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
    EXPECT_EQ(snapshot.backend_attempts_total,
              (std::array<std::uint64_t, 3>{1U, 0U, 0U}));
    EXPECT_EQ(snapshot.attempt_completions_total,
              (std::array<std::uint64_t, 3>{1U, 0U, 0U}));
    EXPECT_EQ(snapshot.retry_started_total, 0U);
    EXPECT_EQ(snapshot.retry_budget_denied_total, 0U);
    EXPECT_EQ(snapshot.retry_budget.consumed_total, 0U);
    EXPECT_EQ(snapshot.retry_budget.denied_total, 0U);
    EXPECT_EQ(snapshot.retry_budget.available, 1U);
    EXPECT_EQ(snapshot.winning_attempts_total,
              (std::array<std::uint64_t, 3>{0U, 0U, 0U}));
    EXPECT_EQ(snapshot.cancelled_attempts_total, 0U);
    EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
    EXPECT_EQ(snapshot.pending_retry_timers, 0U);
    EXPECT_EQ(snapshot.active_attempts, 0U);
    EXPECT_EQ(snapshot.pending_backend_callbacks, 0U);
    EXPECT_EQ(active_timers, 0U);
    ASSERT_NE(controller, nullptr);
    EXPECT_EQ(controller->deadline_misses, 1U);
    EXPECT_EQ(controller->permits_acquired, 1U);
    EXPECT_EQ(controller->permits_released, 1U);
    EXPECT_EQ(controller->route_inflight, 0U);
    for (const auto& replica : controller->replicas) {
      EXPECT_EQ(replica.inflight, 0U);
    }
  }
}

TEST(AttemptManagerIntegrationTest,
     DeadlineDecisionUsesCurrentTimeBeforeLatePrimarySuccess) {
  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend backend("A1");
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend,
                                                &backend_port);
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  artc::rpc::RouterService router(
      {{"A1", address_for(backend_port)}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      controller_config(), {}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  artc::v1::WorkRequest request;
  artc::v1::WorkResponse response;
  grpc::Status status;
  std::thread caller([&] {
    status = execute(address_for(router_port), request, &response, &context);
  });
  const bool primary_entered = backend.wait_for_calls(1, 2s);
  const auto deadline_timer = driver.wait_for_timer(
      artc::rpc::testing::TimerKind::kDeadline);
  driver.pause_next(artc::rpc::testing::Checkpoint::kDeadlineTimerReady);
  std::thread deadline_callback;
  if (deadline_timer) {
    driver.advance_to(*deadline_timer);
    deadline_callback = driver.deliver_async(*deadline_timer);
  }
  const bool callback_paused = deadline_timer && driver.wait_until_paused();
  const auto primary_call = backend.call(0);
  const bool primary_returned = primary_call && primary_call->complete();
  if (!primary_returned) context.TryCancel();
  caller.join();
  driver.release_pause();
  if (deadline_callback.joinable()) deadline_callback.join();
  backend.complete_all({grpc::StatusCode::CANCELLED, "test cleanup"});
  const bool drained = wait_until([&] {
    const auto snapshot = router.attempt_snapshot();
    return snapshot.active_attempts == 0 &&
           snapshot.pending_backend_callbacks == 0 &&
           snapshot.pending_hedge_timers == 0 &&
           snapshot.pending_retry_timers == 0;
  });
  const auto snapshot = router.attempt_snapshot();
  router.begin_shutdown();
  router_server->Shutdown();
  backend_server->Shutdown();

  EXPECT_TRUE(primary_entered);
  EXPECT_TRUE(deadline_timer.has_value());
  EXPECT_TRUE(callback_paused);
  EXPECT_TRUE(primary_returned);
  EXPECT_TRUE(drained);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
  EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
  EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
  EXPECT_EQ(snapshot.attempt_completions_total[0], 1U);
  EXPECT_EQ(snapshot.winning_attempts_total[0], 0U);
  EXPECT_EQ(snapshot.active_attempts, 0U);
  EXPECT_EQ(driver.active_timers(), 0U);
}

TEST(AttemptManagerIntegrationTest,
     SeededManagerEventSequencesCheckBoundsAfterEveryEvent) {
  const std::uint64_t kSeed = attempt_event_seed();
  enum class Event : unsigned {
    kPrimaryWins,
    kHedgeWins,
    kRetrySucceeds,
    kCallerCancels,
    kPermanentFailure,
  };
  constexpr std::size_t kSequences = 15;
  constexpr std::size_t kEventKinds = 5;
  std::array<Event, kSequences> events{};
  std::array<std::uint64_t, kEventKinds> event_counts{};
  const auto random_values = seeded_random_values<kSequences>(kSeed);
  std::array<bool, kSequences> fire_timer_before_terminal{};
  std::array<bool, 2> primary_success_order_coverage{};
  std::array<bool, 2> caller_cancel_order_coverage{};
  for (std::size_t index = 0; index < events.size(); ++index) {
    events[index] = static_cast<Event>(random_values[index] % kEventKinds);
    fire_timer_before_terminal[index] = (random_values[index] & 2U) != 0;
    const auto event = events[index];
    ++event_counts[static_cast<std::size_t>(event)];
    if (event == Event::kPrimaryWins) {
      primary_success_order_coverage[fire_timer_before_terminal[index]] = true;
    } else if (event == Event::kCallerCancels) {
      caller_cancel_order_coverage[fire_timer_before_terminal[index]] = true;
    }
  }
  if (kSeed == 16) {
    for (const auto count : event_counts) EXPECT_GT(count, 0U) << "seed=" << kSeed;
    EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kPrimaryWins)], 3U);
    EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kHedgeWins)], 3U);
    EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kRetrySucceeds)], 3U);
    EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kCallerCancels)], 4U);
    EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kPermanentFailure)], 2U);
    EXPECT_TRUE(primary_success_order_coverage[0]);
    EXPECT_TRUE(primary_success_order_coverage[1]);
    EXPECT_TRUE(caller_cancel_order_coverage[0]);
    EXPECT_TRUE(caller_cancel_order_coverage[1]);
  }

  artc::rpc::testing::AttemptTestDriver driver;
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  std::array<HeldBackend*, 2> backends{&a1, &a2};
  std::array<int, 2> ports{};
  std::array<std::unique_ptr<grpc::Server>, 2> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }
  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.retry_enabled = true;
  method.max_total_attempts = 3;
  method.max_retries = 1;
  method.hedge_delay_min = 5ms;
  method.hedge_delay_max = 5ms;
  method.retry_backoff_base = 5ms;
  method.retry_backoff_max = 5ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = 32, .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = 32, .refill_per_second = 0.0};
  attempts.test_control = driver.control();
  auto config = controller_config();
  config.aimd.control_interval = 1ms;
  // Isolate AttemptManager ordering from replica-health exhaustion across
  // unrelated seeded logical requests in this state-machine campaign.
  config.health.consecutive_failures_to_unavailable = 1'000;
  config.health.minimum_latency_samples = 1'000;
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  for (std::size_t sequence = 0; sequence < kSequences; ++sequence) {
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " sequence="
                                     << sequence << " event="
                                     << static_cast<unsigned>(events[sequence]));
    ASSERT_EQ(driver.active_timers(), 0U);
    const auto before = router.attempt_snapshot();
    const std::array<std::size_t, 2> first_indices{
        a1.call_count(), a2.call_count()};
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 30s);
    artc::v1::WorkRequest request;
    request.set_request_id(sequence + 1);
    artc::v1::WorkResponse response;
    grpc::Status status;
    std::thread caller([&] {
      status = execute(address_for(router_port), request, &response, &context);
    });
    const bool primary_entered =
        wait_for_new_calls(backends, first_indices, 1, 2s);
    const auto hedge_timer = driver.wait_for_timer(
        artc::rpc::testing::TimerKind::kHedge);
    auto request_calls = calls_since(backends, first_indices);
    const auto primary_call = request_calls.empty() ? nullptr : request_calls[0];
    const bool setup = primary_entered && hedge_timer && primary_call;
    if (!setup) {
      context.TryCancel();
      caller.join();
      for (auto* backend : backends) {
        backend->complete_all({grpc::StatusCode::CANCELLED, "setup cleanup"});
      }
      ADD_FAILURE() << "seeded logical request did not start primary and hedge timer";
      break;
    }

    const auto previous_total_attempts = before.backend_attempts_total[0] +
                                         before.backend_attempts_total[1] +
                                         before.backend_attempts_total[2];
    const auto check_invariants = [&](std::string_view stage) {
      const auto snapshot = router.attempt_snapshot();
      const auto total_attempts = snapshot.backend_attempts_total[0] +
                                  snapshot.backend_attempts_total[1] +
                                  snapshot.backend_attempts_total[2];
      const auto total_completions = snapshot.attempt_completions_total[0] +
                                     snapshot.attempt_completions_total[1] +
                                     snapshot.attempt_completions_total[2];
      const auto total_winners = snapshot.winning_attempts_total[0] +
                                 snapshot.winning_attempts_total[1] +
                                 snapshot.winning_attempts_total[2];
      EXPECT_EQ(snapshot.logical_requests_total, sequence + 1U) << stage;
      EXPECT_LE(snapshot.logical_terminal_transitions_total,
                snapshot.logical_requests_total) << stage;
      EXPECT_LE(snapshot.active_attempts, 2U) << stage;
      EXPECT_LE(total_attempts, 3U * snapshot.logical_requests_total) << stage;
      EXPECT_LE(total_attempts - previous_total_attempts, 3U) << stage;
      EXPECT_LE(total_completions, total_attempts) << stage;
      EXPECT_LE(total_winners, snapshot.logical_terminal_transitions_total) << stage;
      EXPECT_GE(snapshot.hedge_budget.tokens, 0.0) << stage;
      EXPECT_GE(snapshot.retry_budget.tokens, 0.0) << stage;
    };
    check_invariants("primary dispatched");

    const auto deliver_stale_hedge = [&] {
      const auto before_stale = router.attempt_snapshot();
      driver.deliver(*hedge_timer);
      const auto after = router.attempt_snapshot();
      EXPECT_EQ(after.backend_attempts_total, before_stale.backend_attempts_total);
      EXPECT_EQ(after.attempt_completions_total,
                before_stale.attempt_completions_total);
      EXPECT_EQ(after.hedge_started_total, before_stale.hedge_started_total);
      EXPECT_EQ(after.hedge_budget_denied_total,
                before_stale.hedge_budget_denied_total);
      EXPECT_EQ(after.hedge_deadline_denied_total,
                before_stale.hedge_deadline_denied_total);
      EXPECT_EQ(after.hedge_overload_denied_total,
                before_stale.hedge_overload_denied_total);
      EXPECT_EQ(after.hedge_no_target_total, before_stale.hedge_no_target_total);
      EXPECT_EQ(after.hedge_attempt_limit_total,
                before_stale.hedge_attempt_limit_total);
      EXPECT_EQ(after.hedge_dispatch_errors_total,
                before_stale.hedge_dispatch_errors_total);
      EXPECT_EQ(after.retry_started_total, before_stale.retry_started_total);
      EXPECT_EQ(after.retry_budget_denied_total,
                before_stale.retry_budget_denied_total);
      EXPECT_EQ(after.retry_deadline_denied_total,
                before_stale.retry_deadline_denied_total);
      EXPECT_EQ(after.retry_not_retryable_total,
                before_stale.retry_not_retryable_total);
      EXPECT_EQ(after.retry_no_target_total, before_stale.retry_no_target_total);
      EXPECT_EQ(after.retry_attempt_limit_total,
                before_stale.retry_attempt_limit_total);
      EXPECT_EQ(after.retry_same_replica_total,
                before_stale.retry_same_replica_total);
      EXPECT_EQ(after.hedge_budget.consumed_total,
                before_stale.hedge_budget.consumed_total);
      EXPECT_EQ(after.hedge_budget.denied_total,
                before_stale.hedge_budget.denied_total);
      EXPECT_EQ(after.hedge_budget.tokens, before_stale.hedge_budget.tokens);
      EXPECT_EQ(after.hedge_budget.available,
                before_stale.hedge_budget.available);
      EXPECT_EQ(after.retry_budget.consumed_total,
                before_stale.retry_budget.consumed_total);
      EXPECT_EQ(after.retry_budget.denied_total,
                before_stale.retry_budget.denied_total);
      EXPECT_EQ(after.retry_budget.tokens, before_stale.retry_budget.tokens);
      EXPECT_EQ(after.retry_budget.available,
                before_stale.retry_budget.available);
      EXPECT_EQ(after.pending_hedge_timers, before_stale.pending_hedge_timers);
      EXPECT_EQ(after.pending_retry_timers, before_stale.pending_retry_timers);
      EXPECT_EQ(after.active_attempts, before_stale.active_attempts);
      EXPECT_EQ(after.cancelled_attempts_total,
                before_stale.cancelled_attempts_total);
      EXPECT_EQ(after.cancellations_by_reason_total,
                before_stale.cancellations_by_reason_total);
      EXPECT_EQ(after.winning_attempts_total,
                before_stale.winning_attempts_total);
      EXPECT_EQ(after.logical_terminal_transitions_total,
                before_stale.logical_terminal_transitions_total);
      check_invariants("stale hedge timer delivered");
    };

    switch (events[sequence]) {
      case Event::kPrimaryWins: {
        std::shared_ptr<HeldBackendCall> hedge_call;
        if (fire_timer_before_terminal[sequence]) {
          driver.advance_to(*hedge_timer);
          driver.deliver(*hedge_timer);
          const bool hedge_entered =
              wait_for_new_calls(backends, first_indices, 2, 2s);
          request_calls = calls_since(backends, first_indices);
          if (hedge_entered && request_calls.size() > 1) {
            hedge_call = request_calls[1];
            check_invariants("hedge dispatched before primary success");
          } else {
            context.TryCancel();
            caller.join();
            ADD_FAILURE() << "seeded primary-win event did not dispatch hedge";
            break;
          }
        }
        EXPECT_TRUE(primary_call->complete());
        caller.join();
        EXPECT_TRUE(status.ok()) << status.error_message();
        if (hedge_call) {
          EXPECT_TRUE(hedge_call->wait_for_cancellation(2s));
          hedge_call->complete({grpc::StatusCode::CANCELLED, "primary won"});
        } else {
          deliver_stale_hedge();
        }
        check_invariants("primary won");
        break;
      }
      case Event::kHedgeWins: {
        driver.advance_to(*hedge_timer);
        driver.deliver(*hedge_timer);
        const bool hedge_entered =
            wait_for_new_calls(backends, first_indices, 2, 2s);
        request_calls = calls_since(backends, first_indices);
        const auto hedge_call = request_calls.size() > 1 ? request_calls[1] : nullptr;
        if (!hedge_entered || !hedge_call) {
          context.TryCancel();
          caller.join();
          ADD_FAILURE() << "seeded hedge event did not dispatch";
          break;
        }
        check_invariants("hedge dispatched");
        EXPECT_TRUE(hedge_call->complete());
        caller.join();
        EXPECT_TRUE(status.ok()) << status.error_message();
        EXPECT_TRUE(primary_call->wait_for_cancellation(2s));
        primary_call->complete({grpc::StatusCode::CANCELLED, "hedge won"});
        check_invariants("hedge completed");
        break;
      }
      case Event::kRetrySucceeds: {
        EXPECT_TRUE(primary_call->complete(
            {grpc::StatusCode::UNAVAILABLE, "seeded retryable failure"}));
        const auto retry_timer = driver.wait_for_timer(
            artc::rpc::testing::TimerKind::kRetry);
        if (!retry_timer) {
          context.TryCancel();
          caller.join();
          ADD_FAILURE() << "seeded retry event did not arm backoff";
          break;
        }
        check_invariants("retry timer armed");
        deliver_stale_hedge();
        driver.advance_to(*retry_timer);
        driver.deliver(*retry_timer);
        const bool retry_entered =
            wait_for_new_calls(backends, first_indices, 2, 2s);
        request_calls = calls_since(backends, first_indices);
        const auto retry_call = request_calls.size() > 1 ? request_calls[1] : nullptr;
        if (!retry_entered || !retry_call) {
          context.TryCancel();
          caller.join();
          ADD_FAILURE() << "seeded retry event did not dispatch";
          break;
        }
        check_invariants("retry dispatched");
        EXPECT_TRUE(retry_call->complete());
        caller.join();
        EXPECT_TRUE(status.ok()) << status.error_message();
        check_invariants("retry completed");
        break;
      }
      case Event::kCallerCancels: {
        std::shared_ptr<HeldBackendCall> hedge_call;
        if (fire_timer_before_terminal[sequence]) {
          driver.advance_to(*hedge_timer);
          driver.deliver(*hedge_timer);
          const bool hedge_entered =
              wait_for_new_calls(backends, first_indices, 2, 2s);
          request_calls = calls_since(backends, first_indices);
          if (hedge_entered && request_calls.size() > 1) {
            hedge_call = request_calls[1];
          } else {
            context.TryCancel();
            caller.join();
            ADD_FAILURE() << "seeded cancellation event did not dispatch hedge";
            break;
          }
        }
        check_invariants("attempts running before caller cancellation");
        context.TryCancel();
        caller.join();
        const bool terminal = wait_until([&] {
          return router.attempt_snapshot().logical_terminal_transitions_total ==
                 sequence + 1U;
        });
        EXPECT_TRUE(terminal);
        EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
        EXPECT_TRUE(primary_call->wait_for_cancellation(2s));
        primary_call->complete({grpc::StatusCode::CANCELLED, "caller cancelled"});
        if (hedge_call) {
          EXPECT_TRUE(hedge_call->wait_for_cancellation(2s));
          hedge_call->complete(
              {grpc::StatusCode::CANCELLED, "caller cancelled"});
        } else {
          deliver_stale_hedge();
        }
        check_invariants("caller cancellation completed");
        break;
      }
      case Event::kPermanentFailure:
        EXPECT_TRUE(primary_call->complete(
            {grpc::StatusCode::INVALID_ARGUMENT, "seeded permanent failure"}));
        caller.join();
        EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
        deliver_stale_hedge();
        break;
    }

    for (auto* backend : backends) {
      backend->complete_all({grpc::StatusCode::CANCELLED, "sequence cleanup"});
    }
    const bool settled = wait_until([&] {
      const auto snapshot = router.attempt_snapshot();
      const auto attempts_started = snapshot.backend_attempts_total[0] +
                                    snapshot.backend_attempts_total[1] +
                                    snapshot.backend_attempts_total[2];
      const auto attempts_completed = snapshot.attempt_completions_total[0] +
                                      snapshot.attempt_completions_total[1] +
                                      snapshot.attempt_completions_total[2];
      const auto controller = router.controller_snapshot();
      return snapshot.active_attempts == 0 &&
             attempts_completed == attempts_started &&
             snapshot.logical_terminal_transitions_total == sequence + 1U &&
             snapshot.pending_hedge_timers == 0 &&
             snapshot.pending_retry_timers == 0 && controller &&
             snapshot.pending_backend_callbacks == 0 &&
             controller->permits_acquired == sequence + 1U &&
             controller->permits_released == sequence + 1U;
    }, 500ms);
    if (!settled) {
      const auto snapshot = router.attempt_snapshot();
      const auto controller = router.controller_snapshot();
      ADD_FAILURE() << "seed=" << kSeed << " sequence=" << sequence
                    << " active=" << snapshot.active_attempts
                    << " attempts=" << snapshot.backend_attempts_total[0] << "/"
                    << snapshot.backend_attempts_total[1] << "/"
                    << snapshot.backend_attempts_total[2]
                    << " completions=" << snapshot.attempt_completions_total[0]
                    << "/" << snapshot.attempt_completions_total[1] << "/"
                    << snapshot.attempt_completions_total[2]
                    << " logical=" << snapshot.logical_terminal_transitions_total
                    << " timers=" << snapshot.pending_hedge_timers << "/"
                    << snapshot.pending_retry_timers
                    << " callbacks=" << snapshot.pending_backend_callbacks
                    << " permits=" << (controller ? controller->permits_acquired : 0)
                    << "/" << (controller ? controller->permits_released : 0);
    }
    check_invariants("logical request drained");
    EXPECT_EQ(driver.active_timers(), 0U);
  }

  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();
}

TEST(AttemptManagerIntegrationTest, SeededCompletionEventSequencesPreserveBounds) {
  constexpr std::uint64_t kSeed = 16;
  enum class Event : unsigned {
    kPrimaryWins,
    kHedgeWins,
    kRetrySucceeds,
    kCallerCancels,
    kPermanentFailure,
  };
  constexpr std::size_t kSequences = 15;
  constexpr std::uint32_t kHedgeBudgetCapacity = 1;
  constexpr std::uint32_t kRetryBudgetCapacity = 1;
  constexpr std::size_t kEventKinds = 5;
  std::array<Event, kSequences> events{};
  std::array<std::uint64_t, kEventKinds> event_counts{};
  const auto random_values = seeded_random_values<kSequences>(kSeed);
  for (std::size_t index = 0; index < events.size(); ++index) {
    events[index] = static_cast<Event>(random_values[index] % kEventKinds);
    const auto event = events[index];
    ++event_counts[static_cast<std::size_t>(event)];
  }
  for (const auto count : event_counts) EXPECT_GT(count, 0U);
  EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kHedgeWins)], 3U);
  EXPECT_EQ(event_counts[static_cast<std::size_t>(Event::kRetrySucceeds)], 3U);
  HeldBackend a1("A1");
  HeldBackend a2("A2");
  HeldBackend a3("A3");
  std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
  constexpr std::array<const char*, 3> replica_ids{"A1", "A2", "A3"};
  std::array<int, 3> ports{};
  std::array<std::unique_ptr<grpc::Server>, 3> backend_servers;
  for (std::size_t index = 0; index < backends.size(); ++index) {
    backend_servers[index] = artc::rpc::start_server(
        "127.0.0.1:0", *backends[index], &ports[index]);
  }

  artc::rpc::MethodPolicy method;
  method.idempotency = artc::rpc::Idempotency::kIdempotent;
  method.hedging_enabled = true;
  method.retry_enabled = true;
  method.max_total_attempts = 3;
  method.max_retries = 1;
  method.hedge_delay_min = 2ms;
  method.hedge_delay_max = 2ms;
  method.retry_backoff_base = 1ms;
  method.retry_backoff_max = 1ms;
  method.retry_jitter_max = 0ms;
  artc::rpc::AttemptRuntimeConfig attempts;
  attempts.hedge_budget = {.capacity = kHedgeBudgetCapacity,
                           .refill_per_second = 0.0};
  attempts.retry_budget = {.capacity = kRetryBudgetCapacity,
                           .refill_per_second = 0.0};
  auto config = controller_config();
  config.aimd.control_interval = 1ms;
  config.health.minimum_latency_samples = 100;
  config.recovery_probe_period = 1'000;
  artc::rpc::RouterService router(
      {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
       {"A3", address_for(ports[2])}},
      artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
      config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                &router_port);

  std::array<std::uint64_t, 3> expected_attempts{};
  std::array<std::uint64_t, 3> expected_winners{};
  std::array<std::uint64_t,
             static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kCount)>
      expected_cancellations{};
  std::uint64_t retry_denials = 0;
  std::uint64_t retry_not_retryable = 0;
  for (std::size_t sequence = 0; sequence < kSequences; ++sequence) {
    const std::array<std::size_t, 3> first_indices{
        a1.call_count(), a2.call_count(), a3.call_count()};
    const auto before = router.attempt_snapshot();
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 3s);
    artc::v1::WorkRequest request;
    request.set_request_id(sequence + 1);
    artc::v1::WorkResponse response;
    grpc::Status status;
    const auto primary_target = static_cast<std::size_t>(request.request_id() % 3U);
    std::thread caller([&] {
      status = execute(address_for(router_port), request, &response, &context);
    });

    const bool primary_dispatched = backends[primary_target]->wait_for_calls(
        first_indices[primary_target] + 1U, 2s);
    const bool hedge_decided = wait_until([&] {
      const auto current = router.attempt_snapshot();
      return current.hedge_started_total + current.hedge_budget_denied_total ==
             sequence + 1U;
    });
    const auto after_hedge_decision = router.attempt_snapshot();
    const bool hedge_started =
        after_hedge_decision.hedge_started_total == before.hedge_started_total + 1U;
    const auto required_initial_calls = hedge_started ? 2U : 1U;
    const bool initial_calls_dispatched =
        !hedge_started || wait_for_new_calls(backends, first_indices,
                                             required_initial_calls, 2s);
    auto calls = calls_since(backends, first_indices);
    const auto primary_call = backends[primary_target]->call(first_indices[primary_target]);
    std::shared_ptr<HeldBackendCall> hedge_call;
    std::optional<std::size_t> hedge_target;
    for (std::size_t target = 0; target < backends.size(); ++target) {
      for (std::size_t index = first_indices[target];
           index < backends[target]->call_count(); ++index) {
        const auto call = backends[target]->call(index);
        if (call && call != primary_call) {
          hedge_call = call;
          hedge_target = target;
        }
      }
    }
    const auto event = events[sequence];
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " sequence="
                                     << sequence << " event="
                                     << static_cast<unsigned>(event));
    const bool dispatches_match = primary_call &&
        calls.size() == required_initial_calls &&
        (hedge_started == static_cast<bool>(hedge_call));
    if (!primary_dispatched || !hedge_decided || !initial_calls_dispatched ||
        !dispatches_match) {
      context.TryCancel();
      caller.join();
      for (auto* backend : backends) {
        backend->complete_all({grpc::StatusCode::CANCELLED, "seeded cleanup"});
      }
      FAIL() << "primary and hedge did not both dispatch";
      break;
    }
    ++expected_attempts[0];
    if (hedge_started) ++expected_attempts[1];
    switch (event) {
      case Event::kPrimaryWins:
        ++expected_winners[0];
        if (hedge_call) ++expected_cancellations[
            static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kWinner)];
        primary_call->complete();
        caller.join();
        if (hedge_call) {
          EXPECT_TRUE(hedge_call->wait_for_cancellation(2s));
          hedge_call->complete({grpc::StatusCode::CANCELLED, "primary won"});
        }
        EXPECT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(trailing_values(context, "artc-winning-attempt-kind"),
                  (std::vector<std::string>{"primary"}));
        EXPECT_EQ(response.replica_id(), replica_ids[primary_target]);
        break;
      case Event::kHedgeWins:
        if (hedge_call) {
          ++expected_winners[1];
          ++expected_cancellations[
              static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kWinner)];
          hedge_call->complete();
        } else {
          ++expected_winners[0];
          primary_call->complete();
        }
        caller.join();
        if (hedge_call) {
          EXPECT_TRUE(primary_call->wait_for_cancellation(2s));
          primary_call->complete({grpc::StatusCode::CANCELLED, "hedge won"});
        }
        EXPECT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(trailing_values(context, "artc-winning-attempt-kind"),
                  (std::vector<std::string>{hedge_call ? "hedge" : "primary"}));
        if (hedge_call) {
          EXPECT_NE(response.replica_id(), replica_ids[primary_target]);
        }
        break;
      case Event::kRetrySucceeds: {
        primary_call->complete(
            {grpc::StatusCode::UNAVAILABLE, "transient primary"});
        if (hedge_call) {
          hedge_call->complete(
              {grpc::StatusCode::UNAVAILABLE, "transient hedge"});
        }
        const bool retry_decided = wait_until([&] {
          const auto current = router.attempt_snapshot();
          return current.retry_started_total + current.retry_budget_denied_total >
                 before.retry_started_total + before.retry_budget_denied_total;
        });
        const auto after_retry_decision = router.attempt_snapshot();
        std::shared_ptr<HeldBackendCall> retry_call;
        std::optional<std::size_t> retry_target;
        if (after_retry_decision.retry_started_total > before.retry_started_total) {
          const auto initial_count = 1U + static_cast<std::size_t>(hedge_started);
          const bool retry_dispatched =
              wait_for_new_calls(backends, first_indices, initial_count + 1U, 2s);
          calls = calls_since(backends, first_indices);
          for (const auto& call : calls) {
            if (call != primary_call && call != hedge_call) retry_call = call;
          }
          for (std::size_t target = 0; target < backends.size(); ++target) {
            for (std::size_t index = first_indices[target];
                 index < backends[target]->call_count(); ++index) {
              if (backends[target]->call(index) == retry_call) retry_target = target;
            }
          }
          if (retry_dispatched && retry_call) {
            ++expected_attempts[2];
            ++expected_winners[2];
            retry_call->complete();
          } else {
            context.TryCancel();
            ADD_FAILURE() << "seed=" << kSeed << " retry event did not dispatch";
          }
          caller.join();
          EXPECT_TRUE(retry_dispatched && retry_call);
          if (retry_dispatched && retry_call) {
            EXPECT_TRUE(status.ok()) << status.error_message();
            EXPECT_EQ(trailing_values(context, "artc-winning-attempt-kind"),
                      (std::vector<std::string>{"retry"}));
            ASSERT_TRUE(retry_target.has_value());
            EXPECT_EQ(response.replica_id(), replica_ids[*retry_target]);
            EXPECT_NE(*retry_target, primary_target);
            if (hedge_started) {
              ASSERT_TRUE(hedge_target.has_value());
              EXPECT_NE(*retry_target, *hedge_target);
            }
          }
        } else {
          ++retry_denials;
          caller.join();
          EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
        }
        EXPECT_TRUE(retry_decided);
        break;
      }
      case Event::kCallerCancels: {
        expected_cancellations[
            static_cast<std::size_t>(artc::rpc::AttemptCancellationReason::kCaller)] +=
            1U + static_cast<std::uint64_t>(hedge_started);
        context.TryCancel();
        caller.join();
        const bool logical_cancelled = wait_until([&] {
          const auto current = router.attempt_snapshot();
          return current.logical_terminal_transitions_total == sequence + 1U &&
                 current.cancellations_by_reason_total[
                     static_cast<std::size_t>(
                         artc::rpc::AttemptCancellationReason::kCaller)] >=
                     expected_cancellations[static_cast<std::size_t>(
                         artc::rpc::AttemptCancellationReason::kCaller)];
        });
        EXPECT_TRUE(logical_cancelled);
        primary_call->complete(
            {grpc::StatusCode::CANCELLED, "caller cancelled"});
        if (hedge_call) {
          hedge_call->complete(
              {grpc::StatusCode::CANCELLED, "caller cancelled"});
        }
        EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
        break;
      }
      case Event::kPermanentFailure:
        ++retry_not_retryable;
        primary_call->complete({grpc::StatusCode::INTERNAL, "permanent primary"});
        if (hedge_call) {
          hedge_call->complete({grpc::StatusCode::UNAVAILABLE, "transient hedge"});
        }
        caller.join();
        EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
        break;
    }

    for (auto* backend : backends) {
      backend->complete_all({grpc::StatusCode::CANCELLED, "seeded cleanup"});
    }
    const auto expected_total_attempts = expected_attempts[0] +
                                         expected_attempts[1] +
                                         expected_attempts[2];
    const bool settled = wait_until([&] {
      const auto current = router.attempt_snapshot();
      const auto controller = router.controller_snapshot();
      const auto completed = current.attempt_completions_total[0] +
                             current.attempt_completions_total[1] +
                             current.attempt_completions_total[2];
      return current.active_attempts == 0 &&
             current.logical_terminal_transitions_total == sequence + 1U &&
             completed == expected_total_attempts && controller &&
             controller->permits_acquired == sequence + 1U &&
             controller->permits_released == sequence + 1U &&
             current.pending_hedge_timers == 0 &&
             current.pending_retry_timers == 0;
    }, 3s);
    EXPECT_TRUE(settled);
    const auto snapshot = router.attempt_snapshot();
    EXPECT_EQ(snapshot.logical_requests_total, sequence + 1U);
    EXPECT_EQ(snapshot.logical_terminal_transitions_total, sequence + 1U);
    EXPECT_EQ(snapshot.backend_attempts_total[0], expected_attempts[0]);
    EXPECT_EQ(snapshot.backend_attempts_total[1], expected_attempts[1]);
    EXPECT_EQ(snapshot.backend_attempts_total[2], expected_attempts[2]);
    EXPECT_EQ(snapshot.attempt_completions_total[0] +
                  snapshot.attempt_completions_total[1] +
                  snapshot.attempt_completions_total[2],
              expected_total_attempts);
    EXPECT_EQ(snapshot.attempt_amplification,
              static_cast<double>(expected_total_attempts) /
                  static_cast<double>(sequence + 1U));
    EXPECT_EQ(snapshot.active_attempts, 0U);
    EXPECT_EQ(snapshot.winning_attempts_total, expected_winners);
    EXPECT_EQ(snapshot.retry_started_total, expected_attempts[2]);
    EXPECT_EQ(snapshot.retry_budget_denied_total, retry_denials);
    EXPECT_EQ(snapshot.retry_not_retryable_total, retry_not_retryable);
    EXPECT_LE(snapshot.hedge_budget.consumed_total, kHedgeBudgetCapacity);
    EXPECT_EQ(snapshot.hedge_budget.denied_total,
              snapshot.hedge_budget_denied_total);
    EXPECT_LE(snapshot.retry_budget.consumed_total, kRetryBudgetCapacity);
    EXPECT_EQ(snapshot.retry_budget.denied_total,
              snapshot.retry_budget_denied_total);
    EXPECT_EQ(snapshot.cancellations_by_reason_total, expected_cancellations);
    EXPECT_GE(snapshot.hedge_budget.tokens, 0.0);
    EXPECT_GE(snapshot.retry_budget.tokens, 0.0);
    EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
    EXPECT_EQ(snapshot.pending_retry_timers, 0U);
  }

  const auto final_snapshot = router.attempt_snapshot();
  EXPECT_EQ(final_snapshot.hedge_budget.consumed_total, kHedgeBudgetCapacity);
  EXPECT_EQ(final_snapshot.hedge_budget_denied_total,
            kSequences - kHedgeBudgetCapacity);
  EXPECT_EQ(final_snapshot.retry_budget.consumed_total, kRetryBudgetCapacity);
  EXPECT_EQ(final_snapshot.retry_budget_denied_total,
            event_counts[static_cast<std::size_t>(Event::kRetrySucceeds)] -
                kRetryBudgetCapacity);

  router.begin_shutdown();
  router_server->Shutdown();
  for (auto& server : backend_servers) server->Shutdown();
}

}  // namespace
