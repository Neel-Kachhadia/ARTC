#include "artc/rpc/services.h"

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

std::string address_for(int port) {
  return "127.0.0.1:" + std::to_string(port);
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

class BlockingBackend final : public artc::v1::Traffic::CallbackService {
 public:
  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext* context,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override {
    {
      std::unique_lock lock(mutex_);
      entered_ = true;
      changed_.notify_all();
      changed_.wait(lock, [this] { return released_; });
    }
    response->set_request_id(request->request_id());
    response->set_replica_id("A1");
    auto* reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest*,
                                   artc::v1::HealthResponse* response) override {
    response->set_ready(true);
    response->set_component_id("A1");
    auto* reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  bool wait_until_entered(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout, [this] { return entered_; });
  }

  void release() {
    std::lock_guard lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool entered_{false};
  bool released_{false};
};

grpc::Status execute(const std::string& address,
                     const artc::v1::WorkRequest& request,
                     artc::v1::WorkResponse* response,
                     std::chrono::milliseconds timeout = 2s) {
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);
  return stub->Execute(&context, request, response);
}

TEST(RpcServicesTest, DependencyCallsAndAllRoutingPoliciesWork) {
  artc::rpc::ServiceB service_b("service-b", 0);
  int service_b_port = 0;
  auto service_b_server = artc::rpc::start_server("127.0.0.1:0", service_b,
                                                  &service_b_port);
  const auto service_b_address = address_for(service_b_port);

  artc::rpc::ServiceA a1("A1", 0, service_b_address);
  artc::rpc::ServiceA a2("A2", 0, service_b_address);
  artc::rpc::ServiceA a3("A3", 0, service_b_address);
  std::array<int, 3> ports{};
  auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &ports[0]);
  auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &ports[1]);
  auto a3_server = artc::rpc::start_server("127.0.0.1:0", a3, &ports[2]);
  std::vector<artc::rpc::ReplicaConfig> replicas{
      {"A1", address_for(ports[0])},
      {"A2", address_for(ports[1])},
      {"A3", address_for(ports[2])}};

  for (const auto policy : {artc::routing::Policy::kRoundRobin,
                            artc::routing::Policy::kLeastInflight,
                            artc::routing::Policy::kEwmaLatency,
                            artc::routing::Policy::kP2CLatencyInflight}) {
    artc::rpc::RouterService router(replicas, policy, 17, 0.2);
    int router_port = 0;
    auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                                 &router_port);
    const auto router_address = address_for(router_port);
    std::vector<std::string> selected;
    for (std::uint64_t id = 1; id <= 6; ++id) {
      artc::v1::WorkRequest request;
      request.set_request_id(id);
      request.set_invoke_dependency(true);
      artc::v1::WorkResponse response;
      const auto status = execute(router_address, request, &response);
      ASSERT_TRUE(status.ok()) << status.error_message();
      EXPECT_EQ(response.request_id(), id);
      EXPECT_EQ(response.backend_attempt_count(), 1U);
      EXPECT_EQ(response.dependency_id(), "service-b");
      EXPECT_TRUE(response.replica_id() == "A1" || response.replica_id() == "A2" ||
                  response.replica_id() == "A3");
      selected.push_back(response.replica_id());
    }
    if (policy == artc::routing::Policy::kRoundRobin) {
      EXPECT_EQ(selected, (std::vector<std::string>{"A1", "A2", "A3", "A1", "A2", "A3"}));
    }
  }
}

TEST(RpcServicesTest, InvalidServiceBRequestReturnsInvalidArgument) {
  artc::rpc::ServiceB service_b("service-b", 1);
  int port = 0;
  auto server = artc::rpc::start_server("127.0.0.1:0", service_b, &port);

  artc::v1::WorkRequest invalid_request;
  invalid_request.set_work_units(10'001);
  artc::v1::WorkResponse response;
  const auto invalid_status = execute(address_for(port), invalid_request, &response);
  EXPECT_EQ(invalid_status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

  artc::v1::WorkRequest valid_request;
  valid_request.set_request_id(9);
  const auto valid_status = execute(address_for(port), valid_request, &response);
  ASSERT_TRUE(valid_status.ok()) << valid_status.error_message();
  EXPECT_EQ(response.replica_id(), "service-b");
}

TEST(RpcServicesTest, HealthReportsReadyComponentIdentity) {
  artc::rpc::ServiceA service_a("A1", 0, {});
  int port = 0;
  auto server = artc::rpc::start_server("127.0.0.1:0", service_a, &port);
  std::string component;

  ASSERT_TRUE(artc::rpc::check_health(address_for(port), &component));
  EXPECT_EQ(component, "A1");
}

TEST(RpcServicesTest, ServiceACancellationCompletesAndAllowsSubsequentCalls) {
  artc::rpc::ServiceA service_a("A1", 1'000, {});
  int port = 0;
  auto server = artc::rpc::start_server("127.0.0.1:0", service_a, &port);
  const auto address = address_for(port);

  artc::v1::WorkRequest request;
  request.set_request_id(1);
  artc::v1::WorkResponse response;
  ASSERT_TRUE(execute(address, request, &response).ok());

  std::size_t deadline_results = 0;
  for (std::uint64_t id = 2; id < 34; ++id) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1ms);
    request.set_request_id(id);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = artc::v1::Traffic::NewStub(channel);
    const auto status = stub->Execute(&context, request, &response);
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::CANCELLED) {
      ++deadline_results;
    } else {
      EXPECT_TRUE(status.ok()) << status.error_message();
    }
  }
  EXPECT_GT(deadline_results, 0U);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 2s);
  request.set_request_id(34);
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  const auto status = stub->Execute(&context, request, &response);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(response.request_id(), 34U);
}

TEST(RpcServicesTest, AdaptiveAdmissionRejectsBeforeDispatchAndExplainsAcceptedRoute) {
  BlockingBackend backend;
  int backend_port = 0;
  auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend,
                                               &backend_port);
  artc::control::ControllerConfig config;
  config.aimd.min_limit = 1;
  config.aimd.max_limit = 1;
  config.aimd.initial_limit = 1;
  config.decision_sample_every = 1;
  std::vector<artc::rpc::ReplicaConfig> replicas{
      {"A1", address_for(backend_port)}};
  artc::rpc::RouterService router(replicas, artc::routing::Policy::kArtcAdaptive,
                                  17, 0.2, config);
  int router_port = 0;
  auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
                                               &router_port);
  const auto address = address_for(router_port);

  grpc::ClientContext accepted_context;
  accepted_context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest accepted_request;
  accepted_request.set_request_id(1);
  artc::v1::WorkResponse accepted_response;
  grpc::Status accepted_status;
  std::thread first([&] {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = artc::v1::Traffic::NewStub(channel);
    accepted_status = stub->Execute(&accepted_context, accepted_request,
                                    &accepted_response);
  });

  if (!backend.wait_until_entered(2s)) {
    backend.release();
    first.join();
    FAIL() << "first request did not reach the backend";
    return;
  }
  grpc::ClientContext rejected_context;
  rejected_context.set_deadline(std::chrono::system_clock::now() + 2s);
  artc::v1::WorkRequest rejected_request;
  rejected_request.set_request_id(2);
  artc::v1::WorkResponse rejected_response;
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  const auto rejected_status = stub->Execute(&rejected_context, rejected_request,
                                             &rejected_response);
  backend.release();
  first.join();

  ASSERT_TRUE(accepted_status.ok()) << accepted_status.error_message();
  EXPECT_EQ(accepted_response.backend_attempt_count(), 1U);
  EXPECT_EQ(trailing_values(accepted_context, "artc-admission-result"),
            (std::vector<std::string>{"ADMITTED"}));
  EXPECT_EQ(trailing_values(accepted_context, "artc-backend-attempts"),
            (std::vector<std::string>{"1"}));
  const auto decision = trailing_values(accepted_context, "artc-decision");
  ASSERT_EQ(decision.size(), 1U);
  EXPECT_NE(decision.front().find("selected=0"), std::string::npos);
  EXPECT_NE(decision.front().find("candidates=0:healthy"), std::string::npos);

  EXPECT_EQ(rejected_status.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
  EXPECT_EQ(trailing_values(rejected_context, "artc-admission-result"),
            (std::vector<std::string>{"REJECT_CONCURRENCY_LIMIT"}));
  EXPECT_EQ(trailing_values(rejected_context, "artc-backend-attempts"),
            (std::vector<std::string>{"0"}));
}

}  // namespace
