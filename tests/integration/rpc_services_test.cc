#include "artc/rpc/services.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

std::string address_for(int port) {
  return "127.0.0.1:" + std::to_string(port);
}

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

}  // namespace
