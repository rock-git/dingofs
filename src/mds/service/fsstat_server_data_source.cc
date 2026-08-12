// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "common/version.h"
#include "mds/background/heartbeat.h"
#include "mds/common/codec.h"
#include "mds/server.h"
#include "mds/service/fsstat_api.h"

namespace dingofs {
namespace mds {

DECLARE_string(mds_storage_engine);

Status ServerManagementDataSource::GetOverview(ManagementOverview& overview) {
  overview.cluster_id = MetaCodec::GetClusterID();
  overview.serving_mds_id = Server::GetInstance().GetMDSMeta().ID();
  overview.storage_engine = FLAGS_mds_storage_engine;
  overview.api_version = "v1";
  overview.build_version = GetGitVersion();
  overview.build_commit = GetGitCommitHash();
  overview.build_commit_time = GetGitCommitTime();
  return Status::OK();
}

Status ServerManagementDataSource::GetFileSystems(Context& ctx, std::vector<pb::mds::FsInfo>& file_systems) {
  return Server::GetInstance().GetFileSystemSet()->GetAllFsInfo(ctx, true, file_systems);
}

Status ServerManagementDataSource::GetMdsNodes(Context& ctx, std::vector<MdsEntry>& mds_nodes) {
  return Server::GetInstance().GetHeartbeat()->GetMDSList(ctx, mds_nodes);
}

Status ServerManagementDataSource::GetClients(std::vector<ClientEntry>& clients) {
  return Server::GetInstance().GetHeartbeat()->GetClientList(clients);
}

Status ServerManagementDataSource::GetCacheMembers(std::vector<CacheMemberEntry>& cache_members) {
  return Server::GetInstance().GetHeartbeat()->GetCacheMemberList(cache_members);
}

bool HandleFsStatApi(brpc::Controller* controller, const std::vector<std::string>& params, butil::IOBufBuilder& os) {
  ServerManagementDataSource data_source;
  return HandleFsStatApi(data_source, controller, params, os);
}

}  // namespace mds
}  // namespace dingofs
