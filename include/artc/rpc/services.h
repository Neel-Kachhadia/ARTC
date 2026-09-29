#pragma once

#include "artc/control/phase2.h"
#include "artc/routing/selector.h"
#include "traffic.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace artc::rpc {

struct ReplicaConfig {
  std::string id;
  std::string address;
};

class ServiceA final : public artc::v1::Traffic::CallbackService {
 public:
  ServiceA(std::string replica_id, std::uint64_t delay_per_work_unit_us,
           std::string dependency_address);
  grpc::ServerUnaryReactor* Execute(grpc::CallbackServerContext* context,
                                    const artc::v1::WorkRequest* request,
                                    artc::v1::WorkResponse* response) override;
  grpc::ServerUnaryReactor* Health(grpc::CallbackServerContext* context,
                                   const artc::v1::HealthRequest* request,
                                   artc::v1::HealthResponse* response) override;

 private:
  std::string replica_id_;
  std::uint64_t delay_per_work_unit_us_;
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
                control::ControllerConfig controller_config = {});
  ~RouterService() override;
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
[[nodiscard]] int wait_for_shutdown(grpc::Server& server);
[[nodiscard]] bool check_health(std::string_view target, std::string* component_id);

}  // namespace artc::rpc
