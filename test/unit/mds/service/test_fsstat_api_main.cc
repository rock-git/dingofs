// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved

#include <gflags/gflags.h>
#include <gtest/gtest.h>

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_heartbeat_mds_offline_period_time_ms, 30 * 1000, "mds offline period time ms");
DEFINE_uint32(mds_heartbeat_client_offline_period_ms, 30 * 1000, "client offline period time ms");
DEFINE_uint32(cache_member_heartbeat_offline_timeout_s, 60, "cache member offline timeout seconds");
DEFINE_uint32(cache_member_heartbeat_miss_timeout_s, 30, "cache member miss timeout seconds");

}  // namespace mds
}  // namespace dingofs

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
