#include "artc/rpc/services.h"

#include <charconv>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

template <typename T>
T parse_integer(std::string_view value) {
  T result{};
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("invalid integer argument");
  }
  return result;
}

double parse_double(std::string_view value) {
  double result = 0.0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("invalid floating-point argument");
  }
  return result;
}

void print_usage() {
  std::cerr << "usage:\n"
            << "  artc_lab_node service-b <listen> <id> <delay-us>\n"
            << "  artc_lab_node service-a <listen> <id> <delay-us> <service-b-address>\n"
            << "  artc_lab_node router <listen> <policy> <seed> <smoothing> <id=address>...\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 2) {
      print_usage();
      return 2;
    }
    artc::rpc::block_shutdown_signals();
    const std::string_view role(argv[1]);

    if (role == "service-b" && argc == 5) {
      artc::rpc::ServiceB service(argv[3], parse_integer<std::uint64_t>(argv[4]));
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }
    if (role == "service-a" && argc == 6) {
      artc::rpc::ServiceA service(argv[3], parse_integer<std::uint64_t>(argv[4]), argv[5]);
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }
    if (role == "router" && argc >= 9) {
      std::vector<artc::rpc::ReplicaConfig> replicas;
      replicas.reserve(static_cast<std::size_t>(argc - 6));
      for (int index = 6; index < argc; ++index) {
        const std::string_view backend(argv[index]);
        const auto separator = backend.find('=');
        if (separator == std::string_view::npos || separator == 0 ||
            separator + 1 == backend.size()) {
          throw std::invalid_argument("backend must use id=address format");
        }
        replicas.push_back({std::string(backend.substr(0, separator)),
                            std::string(backend.substr(separator + 1))});
      }
      artc::rpc::RouterService service(
          std::move(replicas), artc::routing::parse_policy(argv[3]),
          parse_integer<std::uint64_t>(argv[4]), parse_double(argv[5]));
      auto server = artc::rpc::start_server(argv[2], service);
      return artc::rpc::wait_for_shutdown(*server);
    }

    print_usage();
    return 2;
  } catch (const std::exception& error) {
    std::cerr << "artc_lab_node: " << error.what() << '\n';
    return 1;
  }
}
