// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_MDS_SERVICE_FSSTAT_API_H_
#define DINGOFS_MDS_SERVICE_FSSTAT_API_H_

#include <cstdint>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "mds/common/context.h"
#include "mds/common/status.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {

struct ManagementOverview {
  uint64_t cluster_id{0};
  int64_t serving_mds_id{0};
  std::string storage_engine;
  std::string api_version{"v1"};
  std::string build_version;
  std::string build_commit;
  std::string build_commit_time;
};

// Supplies raw management facts to the API module. It deliberately knows
// nothing about JSON, HTTP, cursors, or presentation formatting.
class ManagementDataSource {
 public:
  virtual ~ManagementDataSource() = default;

  virtual Status GetOverview(ManagementOverview& overview) = 0;
  virtual Status GetFileSystems(Context& ctx, std::vector<pb::mds::FsInfo>& file_systems) = 0;
  virtual Status GetMdsNodes(Context& ctx, std::vector<MdsEntry>& mds_nodes) = 0;
  virtual Status GetClients(std::vector<ClientEntry>& clients) = 0;
  virtual Status GetCacheMembers(std::vector<CacheMemberEntry>& cache_members) = 0;
};

// Production adapter for the current in-process MDS state.
class ServerManagementDataSource final : public ManagementDataSource {
 public:
  Status GetOverview(ManagementOverview& overview) override;
  Status GetFileSystems(Context& ctx, std::vector<pb::mds::FsInfo>& file_systems) override;
  Status GetMdsNodes(Context& ctx, std::vector<MdsEntry>& mds_nodes) override;
  Status GetClients(std::vector<ClientEntry>& clients) override;
  Status GetCacheMembers(std::vector<CacheMemberEntry>& cache_members) override;
};

// Handles a recognized /api/v1 route and writes its JSON response. The
// overload taking a data source is the test seam; the transport-only overload
// installs the production adapter.
bool HandleFsStatApi(ManagementDataSource& data_source, brpc::Controller* controller,
                     const std::vector<std::string>& params, butil::IOBufBuilder& os);
bool HandleFsStatApi(brpc::Controller* controller, const std::vector<std::string>& params, butil::IOBufBuilder& os);

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_FSSTAT_API_H_
