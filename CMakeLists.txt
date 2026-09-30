cmake_minimum_required(VERSION 3.28)
project(artc VERSION 0.1.0 LANGUAGES C CXX)

include(CTest)
include(FetchContent)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Git REQUIRED)

set(ARTC_GRPC_TAG v1.84.0)
set(ARTC_GRPC_COMMIT 3252a89f10d8e92997862167ca7d095ecda85973)
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
set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_ENABLE_TESTS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  grpc
  EXCLUDE_FROM_ALL
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG ${ARTC_GRPC_TAG}
  GIT_SHALLOW TRUE
  GIT_SUBMODULES
    third_party/abseil-cpp
    third_party/boringssl-with-bazel
    third_party/cares/cares
    third_party/grpc-proto
    third_party/protobuf
    third_party/re2
    third_party/zlib
  GIT_SUBMODULES_RECURSE FALSE)
FetchContent_MakeAvailable(grpc)
FetchContent_GetProperties(grpc SOURCE_DIR ARTC_GRPC_SOURCE_DIR)
execute_process(
  COMMAND "${GIT_EXECUTABLE}" -C "${ARTC_GRPC_SOURCE_DIR}" rev-parse HEAD
  OUTPUT_VARIABLE ARTC_GRPC_ACTUAL_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE ARTC_GRPC_GIT_RESULT)
if(NOT ARTC_GRPC_GIT_RESULT EQUAL 0 OR
   NOT ARTC_GRPC_ACTUAL_COMMIT STREQUAL ARTC_GRPC_COMMIT)
  message(FATAL_ERROR
    "Expected gRPC ${ARTC_GRPC_TAG} at ${ARTC_GRPC_COMMIT}; found ${ARTC_GRPC_ACTUAL_COMMIT}")
endif()

set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_STATIC ON CACHE BOOL "" FORCE)
FetchContent_Declare(
  hdrhistogram_c
  GIT_REPOSITORY https://github.com/HdrHistogram/HdrHistogram_c.git
  GIT_TAG 0.11.10
  GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(hdrhistogram_c)

if(NOT TARGET hdr_histogram_static)
  message(FATAL_ERROR "HdrHistogram_c 0.11.10 did not define hdr_histogram_static")
endif()

set(ARTC_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${ARTC_GENERATED_DIR}")
set(ARTC_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/traffic.proto")
add_custom_command(
  OUTPUT
    "${ARTC_GENERATED_DIR}/traffic.pb.cc"
    "${ARTC_GENERATED_DIR}/traffic.pb.h"
    "${ARTC_GENERATED_DIR}/traffic.grpc.pb.cc"
    "${ARTC_GENERATED_DIR}/traffic.grpc.pb.h"
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
    --cpp_out=${ARTC_GENERATED_DIR}
    --grpc_out=${ARTC_GENERATED_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
    ${ARTC_PROTO}
  DEPENDS "${ARTC_PROTO}" protobuf::protoc grpc_cpp_plugin
  VERBATIM)

add_library(artc_proto STATIC
  "${ARTC_GENERATED_DIR}/traffic.pb.cc"
  "${ARTC_GENERATED_DIR}/traffic.grpc.pb.cc")
target_include_directories(artc_proto PUBLIC "${ARTC_GENERATED_DIR}")
target_link_libraries(artc_proto PUBLIC protobuf::libprotobuf grpc++)

add_library(artc_core STATIC
  src/control/phase2.cc
  src/routing/selector.cc
  src/bench/arrival_schedule.cc
  src/bench/histogram.cc)
target_include_directories(artc_core PUBLIC include)
target_link_libraries(artc_core PUBLIC hdr_histogram_static)
target_compile_options(artc_core PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
  -Werror=return-type)

add_library(artc_rpc STATIC src/app/services.cc)
target_include_directories(artc_rpc PUBLIC include)
target_link_libraries(artc_rpc PUBLIC artc_core artc_proto grpc++)
target_compile_options(artc_rpc PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
  -Werror=return-type)

add_executable(artc_lab_node src/app/lab_main.cc)
target_link_libraries(artc_lab_node PRIVATE artc_rpc)
target_compile_options(artc_lab_node PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
  -Werror=return-type)

add_executable(artc_loadgen src/bench/loadgen_main.cc)
target_link_libraries(artc_loadgen PRIVATE artc_rpc)
target_compile_options(artc_loadgen PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
  -Werror=return-type)

add_executable(artc_control_bench src/bench/control_bench_main.cc)
target_link_libraries(artc_control_bench PRIVATE artc_core)
target_compile_options(artc_control_bench PRIVATE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
  -Werror=return-type)

if(BUILD_TESTING)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
    GIT_SHALLOW TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  add_executable(artc_unit_tests
    tests/unit/selector_test.cc
    tests/unit/phase2_test.cc
    tests/unit/arrival_schedule_test.cc
    tests/unit/histogram_test.cc
    tests/unit/protocol_test.cc)
  target_link_libraries(artc_unit_tests PRIVATE artc_core artc_proto GTest::gtest_main)
  target_compile_options(artc_unit_tests PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Werror=return-type)
  include(GoogleTest)
  gtest_discover_tests(artc_unit_tests DISCOVERY_MODE PRE_TEST)
  add_executable(artc_attempt_policy_tests tests/unit/attempt_policy_test.cc)
  target_include_directories(artc_attempt_policy_tests PRIVATE include)
  target_link_libraries(artc_attempt_policy_tests PRIVATE artc_proto GTest::gtest_main)
  target_compile_options(artc_attempt_policy_tests PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Werror=return-type)
  gtest_discover_tests(artc_attempt_policy_tests
    DISCOVERY_MODE PRE_TEST TEST_PREFIX AttemptPolicyOnly.)
  add_test(NAME Loadgen.ConstantScheduleValidation
    COMMAND artc_loadgen --validate-schedule --mode constant
            --duration-ms 1000 --rate-rps 100)

  add_executable(artc_integration_tests
    tests/integration/rpc_services_test.cc
    tests/integration/attempt_management_test.cc)
  target_include_directories(artc_integration_tests PRIVATE src/app)
  add_library(artc_rpc_testing STATIC src/app/services.cc)
  target_include_directories(artc_rpc_testing PUBLIC include)
  target_link_libraries(artc_rpc_testing PUBLIC artc_core artc_proto grpc++)
  target_compile_options(artc_rpc_testing PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Werror=return-type)
  target_compile_definitions(artc_rpc_testing PUBLIC ARTC_ENABLE_TEST_HOOKS=1)
  target_link_libraries(artc_integration_tests PRIVATE
    artc_rpc_testing GTest::gtest_main)
  target_compile_options(artc_integration_tests PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Werror=return-type)
  gtest_discover_tests(artc_integration_tests DISCOVERY_MODE PRE_TEST)
endif()
