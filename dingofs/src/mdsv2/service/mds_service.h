// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_SERVICE_H_
#define DINGOFS_MDSV2_SERVICE_H_

#include <cstdint>

#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/common/runnable.h"
#include "dingofs/src/mdsv2/filesystem/filesystem.h"

namespace dingofs {

namespace mdsv2 {

class MDSServiceImpl : public pb::mdsv2::MDSService {
 public:
  MDSServiceImpl(WorkerSetPtr read_worker_set, WorkerSetPtr write_worker_set, FileSystemPtr file_system,
                 IdGeneratorPtr id_generator);

  // fs interface
  void CreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done) override;
  void MountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
               pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done) override;
  void UmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done) override;
  void DeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done) override;
  void GetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                 pb::mdsv2::GetFsInfoResponse* response, google::protobuf::Closure* done) override;

  // dentry interface
  void CreateDentry(google::protobuf::RpcController* controller, const pb::mdsv2::CreateDentryRequest* request,
                    pb::mdsv2::CreateDentryResponse* response, google::protobuf::Closure* done) override;
  void DeleteDentry(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteDentryRequest* request,
                    pb::mdsv2::DeleteDentryResponse* response, google::protobuf::Closure* done) override;
  void GetDentry(google::protobuf::RpcController* controller, const pb::mdsv2::GetDentryRequest* request,
                 pb::mdsv2::GetDentryResponse* response, google::protobuf::Closure* done) override;
  void ListDentry(google::protobuf::RpcController* controller, const pb::mdsv2::ListDentryRequest* request,
                  pb::mdsv2::ListDentryResponse* response, google::protobuf::Closure* done) override;

  // inode interface
  void CreateInode(google::protobuf::RpcController* controller, const pb::mdsv2::CreateInodeRequest* request,
                   pb::mdsv2::CreateInodeResponse* response, google::protobuf::Closure* done) override;

  void CreateRootInode(google::protobuf::RpcController* controller, const pb::mdsv2::CreateRootInodeRequest* request,
                       pb::mdsv2::CreateRootInodeResponse* response, google::protobuf::Closure* done) override;
  void DeleteInode(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteInodeRequest* request,
                   pb::mdsv2::DeleteInodeResponse* response, google::protobuf::Closure* done) override;
  void UpdateInode(google::protobuf::RpcController* controller, const pb::mdsv2::UpdateInodeRequest* request,
                   pb::mdsv2::UpdateInodeResponse* response, google::protobuf::Closure* done) override;
  void UpdateS3Chunk(google::protobuf::RpcController* controller, const pb::mdsv2::UpdateS3ChunkRequest* request,
                     pb::mdsv2::UpdateS3ChunkResponse* response, google::protobuf::Closure* done) override;
  void GetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::BatchGetXAttrRequest* request,
                     pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done) override;

  // high level interface
  void MkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
             pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done) override;

  void MkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
             pb::mdsv2::MkDirResponse* response, google::protobuf::Closure* done) override;
  void RmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
             pb::mdsv2::RmDirResponse* response, google::protobuf::Closure* done) override;

  void Link(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
            pb::mdsv2::LinkResponse* response, google::protobuf::Closure* done) override;
  void UnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
              pb::mdsv2::UnLinkResponse* response, google::protobuf::Closure* done) override;

  void Symlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
               pb::mdsv2::SymlinkResponse* response, google::protobuf::Closure* done) override;
  void ReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                pb::mdsv2::ReadLinkResponse* response, google::protobuf::Closure* done) override;
  void Rename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
              pb::mdsv2::RenameResponse* response, google::protobuf::Closure* done) override;

  // quota interface
  void SetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetFsQuotaRequest* request,
                  pb::mdsv2::SetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsQuotaRequest* request,
                  pb::mdsv2::GetFsQuotaResponse* response, google::protobuf::Closure* done) override;
  void FlushFsUsage(google::protobuf::RpcController* controller, const pb::mdsv2::FlushFsUsageRequest* request,
                    pb::mdsv2::FlushFsUsageResponse* response, google::protobuf::Closure* done) override;

  void SetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetDirQuotaRequest* request,
                   pb::mdsv2::SetDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetDirQuotaRequest* request,
                   pb::mdsv2::GetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void DeleteDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteDirQuotaRequest* request,
                      pb::mdsv2::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void LoadDirQuotas(google::protobuf::RpcController* controller, const pb::mdsv2::LoadDirQuotasRequest* request,
                     pb::mdsv2::LoadDirQuotasResponse* response, google::protobuf::Closure* done) override;
  void FlushDirUsages(google::protobuf::RpcController* controller, const pb::mdsv2::FlushDirUsagesRequest* request,
                      pb::mdsv2::FlushDirUsagesResponse* response, google::protobuf::Closure* done) override;

 private:
  Status GenFsId(int64_t& fs_id);

  void DoCreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                  pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done);
  void DoMountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                 pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done);
  void DoUmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                  pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done);
  void DoDeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                  pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done);
  void DoGetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                   pb::mdsv2::GetFsInfoResponse* response, google::protobuf::Closure* done);

  void DoMkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
               pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done);

  IdGeneratorPtr id_generator_;
  FileSystemPtr file_system_;

  // Run service request.
  WorkerSetPtr read_worker_set_;
  WorkerSetPtr write_worker_set_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_H_