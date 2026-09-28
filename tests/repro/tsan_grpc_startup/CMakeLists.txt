cmake_minimum_required(VERSION 3.25)
project(tsan_grpc_startup LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(ARTC_GRPC_SOURCE_DIR)
  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_ABSL_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_PROTOBUF_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_CARES_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_RE2_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_SSL_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_ZLIB_PROVIDER module CACHE STRING "" FORCE)
  set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
  set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  set(utf8_range_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
  set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  add_subdirectory("${ARTC_GRPC_SOURCE_DIR}" grpc EXCLUDE_FROM_ALL)
  set(PROTOC_TARGET protoc)
  set(GRPC_CPP_PLUGIN_TARGET grpc_cpp_plugin)
  set(GRPC_CPP_TARGET grpc++)
  set(PROTOBUF_TARGET libprotobuf)
else()
  find_package(Protobuf REQUIRED)
  find_package(gRPC REQUIRED CONFIG)
  set(PROTOC_TARGET protobuf::protoc)
  set(GRPC_CPP_PLUGIN_TARGET gRPC::grpc_cpp_plugin)
  set(GRPC_CPP_TARGET gRPC::grpc++)
  set(PROTOBUF_TARGET protobuf::libprotobuf)
endif()

set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/probe.proto")

add_custom_command(
  OUTPUT
    "${GENERATED_DIR}/probe.pb.cc"
    "${GENERATED_DIR}/probe.pb.h"
    "${GENERATED_DIR}/probe.grpc.pb.cc"
    "${GENERATED_DIR}/probe.grpc.pb.h"
  COMMAND $<TARGET_FILE:${PROTOC_TARGET}>
  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
    --cpp_out=${GENERATED_DIR}
    --grpc_out=${GENERATED_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:${GRPC_CPP_PLUGIN_TARGET}>
    ${PROTO_FILE}
  DEPENDS "${PROTO_FILE}" ${PROTOC_TARGET} ${GRPC_CPP_PLUGIN_TARGET}
  VERBATIM)

add_executable(tsan_grpc_startup
  main.cc
  "${GENERATED_DIR}/probe.pb.cc"
  "${GENERATED_DIR}/probe.grpc.pb.cc")
target_include_directories(tsan_grpc_startup PRIVATE "${GENERATED_DIR}")
target_link_libraries(tsan_grpc_startup PRIVATE ${GRPC_CPP_TARGET} ${PROTOBUF_TARGET})
