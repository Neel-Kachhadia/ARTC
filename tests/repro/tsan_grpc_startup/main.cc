#include "probe.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <memory>
#include <string>

namespace {

class ProbeService final : public repro::v1::Probe::CallbackService {
 public:
  grpc::ServerUnaryReactor* Ping(grpc::CallbackServerContext* context,
                                 const repro::v1::PingRequest*,
                                 repro::v1::PingResponse* response) override {
    response->set_message("pong");
    auto* reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }
};

}  // namespace

int main() {
  ProbeService service;
  int port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);

  std::fprintf(stderr, "phase=build_start_begin\n");
  auto server = builder.BuildAndStart();
  if (!server) {
    std::fprintf(stderr, "phase=build_start_failed\n");
    return 1;
  }
  std::fprintf(stderr, "phase=build_start_end port=%d\n", port);

  const std::string target = "127.0.0.1:" + std::to_string(port);
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = repro::v1::Probe::NewStub(channel);
  grpc::ClientContext context;
  repro::v1::PingRequest request;
  repro::v1::PingResponse response;
  std::fprintf(stderr, "phase=client_call_begin\n");
  const grpc::Status status = stub->Ping(&context, request, &response);
  std::fprintf(stderr, "phase=client_call_end status=%d\n",
               static_cast<int>(status.error_code()));

  std::fprintf(stderr, "phase=shutdown_begin\n");
  server->Shutdown();
  server->Wait();
  std::fprintf(stderr, "phase=shutdown_end\n");
  if (!status.ok() || response.message() != "pong") return 2;
  return 0;
}
