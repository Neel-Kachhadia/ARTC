#pragma once

#include "artc/control/phase2.h"
#include "artc/rpc/attempt_budget.h"
#include "artc/rpc/attempt_policy.h"
#include "artc/routing/selector.h"
#include "traffic.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace artc::rpc {

struct ReplicaConfig {
  std::string id;
  std::string address;
};

struct ServiceAWorkMetrics {
  std::atomic<std::uint64_t> started{0};
  std::atomic<std::uint64_t> completed{0};
  std::atomic<std::uint64_t> cancellation_signals{0};
  std::atomic<std::uint64_t> completed_after_cancellation{0};
  std::atomic<std::uint64_t> post_cancel_work_time_us{0};
};

struct ServiceAWorkSnapshot {
  std::uint64_t started{0};
  std::uint64_t completed{0};
  std::uint64_t cancellation_signals{0};
  std::uint64_t completed_after_cancellation{0};
  std::uint64_t post_cancel_work_time_us{0};
};

struct AttemptRuntimeConfig {
  std::uint32_t max_total_attempts{3};
  std::uint32_t max_active_attempts{2};
  std::uint64_t jitter_seed{1};
  std::chrono::microseconds minimum_attempt_budget{1'000};
  AttemptBudgetConfig hedge_budget{.capacity = 10,
                                   .refill_per_second = 1.0};
  AttemptBudgetConfig retry_budget{.capacity = 10,
                                   .refill_per_second = 1.0};
};

enum class AttemptCancellationReason : std::uint8_t {
  kWinner,
  kDeadline,
  kCaller,
  kShutdown,
  kInternal,
  kCount,
};

struct AttemptSnapshot {
  std::uint64_t logical_requests_total{0};
  std::uint64_t logical_terminal_transitions_total{0};
  std::array<std::uint64_t, 3> backend_attempts_total{};
  double attempt_amplification{0.0};
  std::array<std::uint64_t, 3> attempt_completions_total{};
  std::array<std::uint64_t, 3> winning_attempts_total{};
  std::uint64_t cancelled_attempts_total{0};
  std::array<std::uint64_t,
             static_cast<std::size_t>(AttemptCancellationReason::kCount)>
      cancellations_by_reason_total{};
  std::uint64_t wasted_attempt_time_us{0};
  std::uint64_t censored_attempts_total{0};
  std::uint64_t hedge_started_total{0};
  std::uint64_t hedge_budget_denied_total{0};
  std::uint64_t hedge_deadline_denied_total{0};
  std::uint64_t hedge_overload_denied_total{0};
  std::uint64_t hedge_no_target_total{0};
  std::uint64_t hedge_attempt_limit_total{0};
  std::uint64_t hedge_dispatch_errors_total{0};
  std::uint64_t retry_started_total{0};
  std::uint64_t retry_budget_denied_total{0};
  std::uint64_t retry_deadline_denied_total{0};
  std::uint64_t retry_not_retryable_total{0};
  std::uint64_t retry_no_target_total{0};
  std::uint64_t retry_attempt_limit_total{0};
  std::uint64_t retry_same_replica_total{0};
  std::uint64_t pending_hedge_timers{0};
  std::uint64_t pending_retry_timers{0};
  std::uint64_t active_attempts{0};
  AttemptBudgetSnapshot hedge_budget;
  AttemptBudgetSnapshot retry_budget;
  std::uint64_t pending_backend_callbacks{0};
};

class ServiceA final : public artc::v1::Traffic::CallbackService {
 public:
  ServiceA(std::string replica_id, std::uint64_t delay_per_work_unit_us,
           std::string dependency_address,
           std::uint64_t unavailable_first_n = 0,
           bool honor_cancellation = true);
  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext* context,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override;
  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest* request,
                                   artc::v1::HealthResponse* response) override;
  [[nodiscard]] ServiceAWorkSnapshot work_snapshot() const noexcept;

 private:
  std::string replica_id_;
  std::uint64_t delay_per_work_unit_us_;
  std::uint64_t unavailable_first_n_;
  bool honor_cancellation_;
  std::atomic<std::uint64_t> execute_count_{0};
  std::shared_ptr<ServiceAWorkMetrics> work_metrics_{
      std::make_shared<ServiceAWorkMetrics>()};
  std::shared_ptr<artc::v1::Traffic::Stub> dependency_stub_;
};

class ServiceB final : public artc::v1::Traffic::CallbackService {
 public:
  ServiceB(std::string component_id, std::uint64_t delay_per_work_unit_us);
  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext* context,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override;
  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest* request,
                                   artc::v1::HealthResponse* response) override;

 private:
  std::string component_id_;
  std::uint64_t delay_per_work_unit_us_;
};

class RouterService final : public artc::v1::Traffic::CallbackService {
 public:
  struct State;
  RouterService(std::vector<ReplicaConfig> replicas, routing::Policy policy,
                std::uint64_t seed, double ewma_smoothing,
                control::ControllerConfig controller_config = {},
                std::unordered_map<std::string, MethodPolicy> method_policies = {},
                AttemptRuntimeConfig attempt_config = {});
  ~RouterService() override;
  void begin_shutdown() noexcept;
  [[nodiscard]] AttemptSnapshot attempt_snapshot() const noexcept;
  [[nodiscard]] std::shared_ptr<const control::ControllerSnapshot>
  controller_snapshot() const noexcept;
  [[nodiscard]] bool wait_for_attempt_callbacks(
      std::chrono::steady_clock::time_point deadline) const;
  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext* context,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override;
  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest* request,
                                   artc::v1::HealthResponse* response) override;

 private:
  std::shared_ptr<State> state_;
};

[[nodiscard]] std::unique_ptr<grpc::Server> start_server(
    std::string_view address, grpc::Service& service, int* selected_port = nullptr);
void block_shutdown_signals();
[[nodiscard]] int wait_for_shutdown(grpc::Server& server,
                                    RouterService* router = nullptr);
[[nodiscard]] bool check_health(std::string_view target, std::string* component_id);

}  // namespace artc::rpc
