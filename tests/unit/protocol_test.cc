#include "traffic.pb.h"

#include <cstdint>
#include <limits>
#include <string>

#include <gtest/gtest.h>

namespace {

TEST(ProtocolTest, RoundTripsBoundaryValuesAndDependencyFlag) {
  artc::v1::WorkRequest request;
  request.set_request_id(std::numeric_limits<std::uint64_t>::max());
  request.set_work_units(std::numeric_limits<std::uint32_t>::max());
  request.set_payload_bytes(0);
  request.set_invoke_dependency(true);
  std::string wire;
  ASSERT_TRUE(request.SerializeToString(&wire));
  artc::v1::WorkRequest decoded;
  ASSERT_TRUE(decoded.ParseFromString(wire));
  EXPECT_EQ(decoded.request_id(), request.request_id());
  EXPECT_EQ(decoded.work_units(), request.work_units());
  EXPECT_EQ(decoded.payload_bytes(), 0U);
  EXPECT_TRUE(decoded.invoke_dependency());
  EXPECT_FALSE(decoded.ParseFromString("\xff"));
}

}  // namespace
