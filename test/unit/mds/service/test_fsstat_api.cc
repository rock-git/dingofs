// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>
#include <json/reader.h>
#include <json/value.h>

#include <limits>
#include <string>

#include "mds/service/fsstat_api.h"
#include "mds/service/fsstat_assets.h"

namespace dingofs {
namespace mds {
namespace unit_test {
namespace {

class FakeManagementDataSource final : public ManagementDataSource {
 public:
  Status GetOverview(ManagementOverview& overview) override {
    overview.cluster_id = 7;
    overview.serving_mds_id = 11;
    overview.storage_engine = "dummy";
    overview.build_version = "test-version";
    overview.build_commit = "test-commit";
    overview.build_commit_time = "test-time";
    return Status::OK();
  }

  Status GetFileSystems(Context&,
                        std::vector<pb::mds::FsInfo>& file_systems) override {
    pb::mds::FsInfo fs_info;
    fs_info.set_fs_id(12);
    fs_info.set_fs_name("test-fs");
    fs_info.set_capacity(std::numeric_limits<uint64_t>::max());
    fs_info.set_status(pb::mds::FsStatus::NORMAL);
    auto* s3_info = fs_info.mutable_extra()->mutable_s3_info();
    s3_info->set_ak("sentinel-access-key");
    s3_info->set_sk("sentinel-secret-key");
    s3_info->set_endpoint("https://s3.example");
    s3_info->set_bucketname("bucket");
    file_systems.push_back(fs_info);
    return Status::OK();
  }

  Status GetMdsNodes(Context&, std::vector<MdsEntry>&) override {
    return Status::OK();
  }
  Status GetClients(std::vector<ClientEntry>&) override { return Status::OK(); }
  Status GetCacheMembers(std::vector<CacheMemberEntry>&) override {
    return Status::OK();
  }
};

std::string ResponseBody(butil::IOBufBuilder& builder) {
  butil::IOBuf response;
  builder.move_to(response);
  std::string body;
  response.copy_to(&body);
  return body;
}

}  // namespace

TEST(FsStatAssetsTest, EmbedsEntryAsset) {
  const auto* index = FindFsStatAsset("index.html");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(index->content_type, "text/html; charset=utf-8");
  EXPECT_GT(index->size, 0);
  EXPECT_FALSE(index->etag.empty());
}

TEST(FsStatAssetsTest, DoesNotFindUnknownAsset) {
  EXPECT_EQ(FindFsStatAsset("assets/missing.js"), nullptr);
}

TEST(FsStatAssetsTest, ServesMigratedEntryRoutes) {
  for (const std::string route :
       {"server-details", "version", "locks", "tools/parse-key",
        "filesystems/12/details", "filesystems/12/tree",
        "filesystems/12/deleted-files/99"}) {
    brpc::Controller controller;
    butil::IOBufBuilder builder;
    EXPECT_TRUE(ServeFsStatConsole(&controller, route, builder)) << route;
  }
  brpc::Controller controller;
  butil::IOBufBuilder builder;
  EXPECT_FALSE(ServeFsStatConsole(&controller, "server", builder));
  EXPECT_FALSE(ServeFsStatConsole(&controller, "legacy", builder));
}

TEST(FsStatApiTest, UsesDataSourceAndRedactsStorageSecrets) {
  FakeManagementDataSource data_source;
  brpc::Controller controller;
  controller.http_request().set_method(brpc::HTTP_METHOD_GET);
  butil::IOBufBuilder builder;

  ASSERT_TRUE(HandleFsStatApi(data_source, &controller,
                              {"api", "v1", "filesystems"}, builder));
  EXPECT_EQ(controller.http_response().status_code(), brpc::HTTP_STATUS_OK);

  const auto body = ResponseBody(builder);
  EXPECT_EQ(body.find("sentinel-access-key"), std::string::npos);
  EXPECT_EQ(body.find("sentinel-secret-key"), std::string::npos);

  Json::Value response;
  Json::Reader reader;
  ASSERT_TRUE(reader.parse(body, response));
  ASSERT_EQ(response["items"].size(), 1);
  EXPECT_EQ(response["items"][0]["id"].asString(), "12");
  EXPECT_EQ(response["items"][0]["capacityBytes"].asString(),
            "18446744073709551615");
  EXPECT_EQ(response["items"][0]["storage"]["bucket"].asString(), "bucket");
}

TEST(FsStatApiTest, RejectsNonGetRequestsWithStructuredError) {
  FakeManagementDataSource data_source;
  brpc::Controller controller;
  controller.http_request().set_method(brpc::HTTP_METHOD_POST);
  butil::IOBufBuilder builder;

  ASSERT_TRUE(HandleFsStatApi(data_source, &controller,
                              {"api", "v1", "overview"}, builder));
  EXPECT_EQ(controller.http_response().status_code(),
            brpc::HTTP_STATUS_METHOD_NOT_ALLOWED);
  const auto body = ResponseBody(builder);
  EXPECT_NE(body.find("method_not_allowed"), std::string::npos);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
