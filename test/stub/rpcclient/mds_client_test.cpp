/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */

#include "stub/rpcclient/mds_client.h"

#include <brpc/server.h>
#include <gmock/gmock-more-actions.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>

#include "dingofs/topology.pb.h"
#include "stub/common/config.h"
#include "stub/rpcclient/mock_mds_base_client.h"
#include "stub/rpcclient/mock_mds_service.h"
#include "utils/protobuf_message_utils.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
using ::testing::_;
using ::testing::Invoke;
using ::testing::SetArgPointee;

using common::CopysetID;
using common::LogicPoolID;
using common::MetaserverID;
using common::PartitionID;

using pb::common::PartitionInfo;
using pb::common::Peer;
using pb::mds::CommitTxRequest;
using pb::mds::CommitTxResponse;
using pb::mds::FsInfo;
using pb::mds::FSStatusCode;
using pb::mds::GetLatestTxIdRequest;
using pb::mds::GetLatestTxIdResponse;
using pb::mds::Mountpoint;
using pb::mds::topology::Copyset;
using pb::mds::topology::PartitionTxId;
using pb::mds::topology::TopoStatusCode;

void MountFsRpcFailed(const std::string& fsName,
                      const pb::mds::Mountpoint& mountPt,
                      pb::mds::MountFsResponse* response,
                      brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void UmountFsRpcFailed(const std::string& fsName,
                       const pb::mds::Mountpoint& mountPt,
                       pb::mds::UmountFsResponse* response,
                       brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void AllocOrGetMemcacheClusterRpcFailed(
    uint32_t fsId,
    pb::mds::topology::AllocOrGetMemcacheClusterResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void GetFsInfoByFsnameRpcFailed(const std::string& fsName,
                                pb::mds::GetFsInfoResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void GetFsInfoByFsIDRpcFailed(uint32_t fsId,
                              pb::mds::GetFsInfoResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void GetMetaServerInfoRpcFailed(
    uint32_t port, std::string ip,
    pb::mds::topology::GetMetaServerInfoResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void GetMetaServerListInCopysetsRpcFailed(
    const common::LogicPoolID& logicalpooid,
    const std::vector<common::CopysetID>& copysetidvec,
    pb::mds::topology::GetMetaServerListInCopySetsResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void CreatePartitionRpcFailed(
    uint32_t fsID, uint32_t count,
    pb::mds::topology::CreatePartitionResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void GetCopysetOfPartitionsRpcFailed(
    const std::vector<uint32_t>& partitionIDList,
    pb::mds::topology::GetCopysetOfPartitionResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void ListPartitionRpcFailed(uint32_t fsID,
                            pb::mds::topology::ListPartitionResponse* response,
                            brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

void RefreshSessionRpcFailed(const pb::mds::RefreshSessionRequest& request,
                             pb::mds::RefreshSessionResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel) {
  cntl->SetFailed(112, "Not connected to");
}

bool ComparePartition(pb::common::PartitionInfo first,
                      pb::common::PartitionInfo second) {
  return first.fsid() == second.fsid() && first.poolid() == second.poolid() &&
         first.copysetid() == second.copysetid() &&
         first.partitionid() == second.partitionid() &&
         first.start() == second.start() && first.end() == second.end();
}

bool CompareCopyset(pb::mds::topology::Copyset first,
                    pb::mds::topology::Copyset second) {
  return first.poolid() == second.poolid() &&
         first.copysetid() == second.copysetid();
}

class MdsClientImplTest : public testing::Test {
 protected:
  void SetUp() override {
    stub::common::MdsOption mdsopt;
    mdsopt.rpcRetryOpt.addrs = {addr_};
    mdsopt.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
    mdsopt.rpcRetryOpt.maxRPCTimeoutMS = 1000;        // 1s
    mdsopt.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
    mdsopt.mdsMaxRetryMS = 2000;                      // 2s
    mdsopt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;

    ASSERT_EQ(pb::mds::FSStatusCode::OK,
              mdsclient_.Init(mdsopt, &mockmdsbasecli_));

    ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
  }

  void TearDown() override {
    server_.Stop(0);
    server_.Join();
  }

 protected:
  MdsClientImpl mdsclient_;
  MockMDSBaseClient mockmdsbasecli_;

  MockMdsService mockMdsService_;
  std::string addr_ = "127.0.0.1:5602";
  brpc::Server server_;
};

TEST_F(MdsClientImplTest, test_MountFs) {
  std::string fsName = "test1";
  pb::mds::Mountpoint mp;
  mp.set_hostname("0.0.0.0");
  mp.set_port(9000);
  mp.set_path("/data");
  pb::mds::FsInfo out;

  pb::mds::MountFsResponse response;
  auto* fsinfo = new pb::mds::FsInfo();
  fsinfo->set_fsid(1);
  fsinfo->set_fsname(fsName);
  fsinfo->set_rootinodeid(1);
  fsinfo->set_capacity(10 * 1024 * 1024L);
  fsinfo->set_block_size(4 * 1024);
  fsinfo->set_chunk_size(16 * 1024);
  fsinfo->set_mountnum(1);
  auto* storage_info = fsinfo->mutable_storage_info();
  storage_info->set_type(pb::common::StorageType::TYPE_S3);
  auto* s3_info = storage_info->mutable_s3_info();
  s3_info->set_ak("a");
  s3_info->set_sk("b");
  s3_info->set_endpoint("http://127.0.1:9000");
  s3_info->set_bucketname("test");

  pb::mds::Mountpoint mountPoint;
  mountPoint.set_hostname("0.0.0.0");
  mountPoint.set_port(9000);
  mountPoint.set_path("/data");
  *fsinfo->add_mountpoints() = mountPoint;
  response.set_allocated_fsinfo(fsinfo);

  // 1. mount ok
  response.set_statuscode(pb::mds::FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_EQ(FSStatusCode::OK, mdsclient_.MountFs(fsName, mp, &out));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 2. mount point not exist
  out.Clear();
  response.set_statuscode(pb::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST);
  EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_EQ(FSStatusCode::MOUNT_POINT_NOT_EXIST,
            mdsclient_.MountFs(fsName, mp, &out));
  ASSERT_FALSE(
      google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 3. umount rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
      .WillRepeatedly(Invoke(MountFsRpcFailed));
  ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.MountFs(fsName, mp, &out));
}

TEST_F(MdsClientImplTest, test_UmountFs) {
  std::string fsName = "test1";
  Mountpoint mp;
  mp.set_hostname("0.0.0.0");
  mp.set_port(9000);
  mp.set_path("/data");
  pb::mds::UmountFsResponse response;

  // 1. umount ok
  response.set_statuscode(pb::mds::FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_EQ(FSStatusCode::OK, mdsclient_.UmountFs(fsName, mp));

  // 2. umount unknown error
  response.set_statuscode(pb::mds::FSStatusCode::UNKNOWN_ERROR);
  EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.UmountFs(fsName, mp));

  // 3. umount rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
      .WillRepeatedly(Invoke(UmountFsRpcFailed));
  ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.UmountFs(fsName, mp));
}

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsname) {
  std::string fsName = "test1";
  FsInfo out;

  pb::mds::GetFsInfoResponse response;
  auto* fsinfo = new pb::mds::FsInfo();
  fsinfo->set_fsid(1);
  fsinfo->set_fsname(fsName);
  fsinfo->set_rootinodeid(1);
  fsinfo->set_capacity(10 * 1024 * 1024L);
  fsinfo->set_block_size(4 * 1024);
  fsinfo->set_chunk_size(16 * 1024);
  fsinfo->set_mountnum(1);
  auto* storage_info = fsinfo->mutable_storage_info();
  storage_info->set_type(pb::common::StorageType::TYPE_S3);
  auto* s3_info = storage_info->mutable_s3_info();
  s3_info->set_ak("a");
  s3_info->set_sk("b");
  s3_info->set_endpoint("http://127.0.1:9000");
  s3_info->set_bucketname("test");

  Mountpoint mountPoint;
  mountPoint.set_hostname("0.0.0.0");
  mountPoint.set_port(9000);
  mountPoint.set_path("/data");
  *fsinfo->add_mountpoints() = mountPoint;
  response.set_allocated_fsinfo(fsinfo);

  // 1. get fsinfo ok
  response.set_statuscode(pb::mds::FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsName, &out));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 2. get fsinfo not found
  out.Clear();
  response.set_statuscode(pb::mds::FSStatusCode::NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_EQ(FSStatusCode::NOT_FOUND, mdsclient_.GetFsInfo(fsName, &out));
  ASSERT_FALSE(
      google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 3. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
      .WillRepeatedly(Invoke(GetFsInfoByFsnameRpcFailed));
  ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsName, &out));
}

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsid) {
  uint32_t fsid = 1;
  FsInfo out;

  pb::mds::GetFsInfoResponse response;
  auto* fsinfo = new pb::mds::FsInfo();
  fsinfo->set_fsid(1);
  fsinfo->set_fsname("test1");
  fsinfo->set_rootinodeid(1);
  fsinfo->set_capacity(10 * 1024 * 1024L);
  fsinfo->set_block_size(4 * 1024);
  fsinfo->set_chunk_size(16 * 1024);
  fsinfo->set_mountnum(1);
  auto* storage_info = fsinfo->mutable_storage_info();
  storage_info->set_type(pb::common::StorageType::TYPE_S3);
  auto* s3_info = storage_info->mutable_s3_info();
  s3_info->set_ak("a");
  s3_info->set_sk("b");
  s3_info->set_endpoint("http://127.0.1:9000");
  s3_info->set_bucketname("test");

  Mountpoint mountPoint;
  mountPoint.set_hostname("0.0.0.0");
  mountPoint.set_port(9000);
  mountPoint.set_path("/data");
  *fsinfo->add_mountpoints() = mountPoint;
  response.set_allocated_fsinfo(fsinfo);

  // 1. get file info ok
  response.set_statuscode(pb::mds::FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsid, &out));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 2. get fsinfo unknow error
  out.Clear();
  response.set_statuscode(pb::mds::FSStatusCode::UNKNOWN_ERROR);
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.GetFsInfo(fsid, &out));
  ASSERT_FALSE(
      google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

  // 3. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
      .WillRepeatedly(Invoke(GetFsInfoByFsIDRpcFailed));
  ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsid, &out));
}

TEST_F(MdsClientImplTest, CommitTx) {
  pb::mds::CommitTxResponse response;

  // CASE 1: CommitTx success
  response.set_statuscode(FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  auto txIds = std::vector<pb::mds::topology::PartitionTxId>();
  auto rc = mdsclient_.CommitTx(txIds);
  ASSERT_EQ(rc, FSStatusCode::OK);

  // CASE 2: CommitTx fail
  response.set_statuscode(FSStatusCode::UNKNOWN_ERROR);
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  rc = mdsclient_.CommitTx(txIds);
  ASSERT_EQ(rc, FSStatusCode::UNKNOWN_ERROR);

  // CASE 3: RPC error, retry until success
  int count = 0;
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .Times(6)
      .WillRepeatedly(
          Invoke([&](const pb::mds::CommitTxRequest& request,
                     pb::mds::CommitTxResponse* response,
                     brpc::Controller* cntl, brpc::Channel* channel) {
            if (++count <= 5) {
              cntl->SetFailed(112, "Not connected to");
            } else {
              response->set_statuscode(FSStatusCode::OK);
            }
          }));

  rc = mdsclient_.CommitTx(txIds);
  ASSERT_EQ(rc, FSStatusCode::OK);
}

TEST_F(MdsClientImplTest, CommitTxWithLock) {
  std::vector<PartitionTxId> txIds;
  std::string fsName = "/test";
  std::string uuid = "uuid";
  uint64_t txSequence = 100;

  // CASE 1: CommitTx success
  CommitTxResponse response;
  response.set_statuscode(FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  auto rc = mdsclient_.CommitTxWithLock(txIds, fsName, uuid, txSequence);
  ASSERT_EQ(rc, FSStatusCode::OK);

  // CASE 2: CommitTx fail
  response.set_statuscode(FSStatusCode::UNKNOWN_ERROR);
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  rc = mdsclient_.CommitTxWithLock(txIds, fsName, uuid, txSequence);
  ASSERT_EQ(rc, FSStatusCode::UNKNOWN_ERROR);

  // CASE 3: RPC error or acquire dlock fail/timeout, retry until success
  int count = 0;
  EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
      .Times(6)
      .WillRepeatedly(
          Invoke([&](const CommitTxRequest& request, CommitTxResponse* response,
                     brpc::Controller* cntl, brpc::Channel* channel) {
            ASSERT_EQ(request.lock(), true);
            ASSERT_EQ(request.fsname(), fsName);
            ASSERT_EQ(request.uuid(), uuid);
            ASSERT_EQ(request.txsequence(), txSequence);
            ++count;
            if (count == 1) {
              response->set_statuscode(FSStatusCode::LOCK_TIMEOUT);
            } else if (count == 2) {
              response->set_statuscode(FSStatusCode::LOCK_FAILED);
            } else if (count <= 5) {
              cntl->SetFailed(112, "Not connected to");
            } else {
              response->set_statuscode(FSStatusCode::OK);
            }
          }));

  rc = mdsclient_.CommitTxWithLock(txIds, fsName, uuid, txSequence);
  ASSERT_EQ(rc, FSStatusCode::OK);
}

TEST_F(MdsClientImplTest, GetLatestTxId) {
  std::vector<PartitionTxId> txIds;
  uint32_t fsId = 1;

  // CASE 1: GetLatestTxId success
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .WillOnce(Invoke([&](const GetLatestTxIdRequest& request,
                           GetLatestTxIdResponse* response,
                           brpc::Controller* cntl, brpc::Channel* channel) {
        if (request.fsid() != fsId) {
          response->set_statuscode(FSStatusCode::PARAM_ERROR);
        } else {
          response->set_statuscode(FSStatusCode::OK);
        }
      }));

  auto rc = mdsclient_.GetLatestTxId(fsId, &txIds);
  ASSERT_EQ(rc, FSStatusCode::OK);

  // CASE 2: GetLatestTxId fail
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .WillOnce(Invoke([&](const GetLatestTxIdRequest& request,
                           GetLatestTxIdResponse* response,
                           brpc::Controller* cntl, brpc::Channel* channel) {
        if (request.fsid() != fsId) {
          response->set_statuscode(FSStatusCode::PARAM_ERROR);
        } else {
          response->set_statuscode(FSStatusCode::UNKNOWN_ERROR);
        }
      }));

  rc = mdsclient_.GetLatestTxId(fsId, &txIds);
  ASSERT_EQ(rc, FSStatusCode::UNKNOWN_ERROR);

  // CASE 3: RPC error, retry until success
  int count = 0;
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .Times(6)
      .WillRepeatedly(
          Invoke([&](const GetLatestTxIdRequest& request,
                     GetLatestTxIdResponse* response, brpc::Controller* cntl,
                     brpc::Channel* channel) {
            if (request.fsid() != fsId) {
              response->set_statuscode(FSStatusCode::PARAM_ERROR);
            } else if (++count <= 5) {
              cntl->SetFailed(112, "Not connected to");
            } else {
              response->set_statuscode(FSStatusCode::OK);
            }
          }));

  rc = mdsclient_.GetLatestTxId(fsId, &txIds);
  ASSERT_EQ(rc, FSStatusCode::OK);
}

TEST_F(MdsClientImplTest, GetLatestTxIdWithLock) {
  std::vector<PartitionTxId> txIds;
  uint32_t fsId;
  std::string fsName = "/test";
  std::string uuid = "uuid";
  uint64_t sequence;

  // CASE 1: GetLatestTxId success
  GetLatestTxIdResponse response;
  response.set_statuscode(FSStatusCode::OK);
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  auto rc =
      mdsclient_.GetLatestTxIdWithLock(fsId, fsName, uuid, &txIds, &sequence);
  ASSERT_EQ(rc, FSStatusCode::OK);

  // CASE 2: GetLatestTxId fail
  response.set_statuscode(FSStatusCode::UNKNOWN_ERROR);
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));

  rc = mdsclient_.GetLatestTxIdWithLock(fsId, fsName, uuid, &txIds, &sequence);
  ASSERT_EQ(rc, FSStatusCode::UNKNOWN_ERROR);

  // CASE 3: RPC error or acquire dlock fail/timeout, retry until success
  int count = 0;
  EXPECT_CALL(mockmdsbasecli_, GetLatestTxId(_, _, _, _))
      .Times(6)
      .WillRepeatedly(
          Invoke([&](const GetLatestTxIdRequest& request,
                     GetLatestTxIdResponse* response, brpc::Controller* cntl,
                     brpc::Channel* channel) {
            ASSERT_EQ(request.lock(), true);
            ASSERT_EQ(request.fsid(), fsId);
            ASSERT_EQ(request.fsname(), fsName);
            ASSERT_EQ(request.uuid(), uuid);
            ++count;
            if (count == 1) {
              response->set_statuscode(FSStatusCode::LOCK_TIMEOUT);
            } else if (count == 2) {
              response->set_statuscode(FSStatusCode::LOCK_FAILED);
            } else if (count <= 5) {
              cntl->SetFailed(112, "Not connected to");
            } else {
              response->set_statuscode(FSStatusCode::OK);
              response->set_txsequence(100);
            }
          }));

  rc = mdsclient_.GetLatestTxIdWithLock(fsId, fsName, uuid, &txIds, &sequence);
  ASSERT_EQ(rc, FSStatusCode::OK);
  ASSERT_EQ(sequence, 100);
}

TEST_F(MdsClientImplTest, test_GetMetaServerInfo) {
  // in
  stub::common::EndPoint ep;
  butil::str2endpoint("127.0.0.1", 5000, &ep);
  stub::common::PeerAddr addr(ep);

  // out
  common::CopysetPeerInfo<MetaserverID> out;

  pb::mds::topology::GetMetaServerInfoResponse response;
  auto metaserverInfo = new pb::mds::topology::MetaServerInfo();
  metaserverInfo->set_metaserverid(1);
  metaserverInfo->set_hostname("hangzhou");
  metaserverInfo->set_internalip("127.0.0.1");
  metaserverInfo->set_internalport(5000);
  metaserverInfo->set_externalip("127.0.0.1");
  metaserverInfo->set_externalport(5000);
  metaserverInfo->set_onlinestate(pb::mds::topology::ONLINE);
  response.set_allocated_metaserverinfo(metaserverInfo);

  // 1. get metaserver info ok
  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_TRUE(mdsclient_.GetMetaServerInfo(addr, &out));
  ASSERT_TRUE(out == common::CopysetPeerInfo<MetaserverID>(
                         1, common::PeerAddr(ep), common::PeerAddr(ep)));

  // 2. get metaserver info not found
  response.set_statuscode(TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));

  // 3. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
      .WillRepeatedly(Invoke(GetMetaServerInfoRpcFailed));
  ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));
}

TEST_F(MdsClientImplTest, GetMetaServerListInCopysets) {
  // in
  LogicPoolID poolID = 1;
  std::vector<CopysetID> copysetidvec{1};

  // out
  std::vector<common::CopysetInfo<MetaserverID>> out;

  pb::mds::topology::GetMetaServerListInCopySetsResponse response;
  auto copysetInfo = response.add_csinfo();
  copysetInfo->set_copysetid(1);
  auto l1 = copysetInfo->add_cslocs();
  auto l2 = copysetInfo->add_cslocs();
  auto l3 = copysetInfo->add_cslocs();
  l1->set_metaserverid(1);
  l1->set_internalip("127.0.0.1");
  l1->set_internalport(9000);
  l1->set_externalip("127.0.0.1");
  l2->CopyFrom(*l1);
  l2->set_metaserverid(2);
  l3->CopyFrom(*l1);
  l3->set_metaserverid(3);

  // 1. get metaserver list in copysets ok
  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_TRUE(
      mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
  ASSERT_EQ(1, out.size());
  ASSERT_EQ(3, out[0].csinfos_.size());
  std::list<int> ids{1, 2, 3};
  for (int i = 0; i <= 2; i++) {
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), out[0].csinfos_[i].peerID) !=
                ids.end());
  }

  // 2. get metaserver list in copyset internal error
  response.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_FALSE(
      mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));

  // 3. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
      .WillRepeatedly(Invoke(GetMetaServerListInCopysetsRpcFailed));
  ASSERT_FALSE(
      mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
}

TEST_F(MdsClientImplTest, CreatePartition) {
  // in
  uint32_t fsID = 1;
  uint32_t count = 2;

  // out
  std::vector<PartitionInfo> out;

  PartitionInfo partitioninfo1;
  PartitionInfo partitioninfo2;
  partitioninfo1.set_fsid(fsID);
  partitioninfo1.set_poolid(1);
  partitioninfo1.set_copysetid(2);
  partitioninfo1.set_partitionid(3);
  partitioninfo1.set_start(4);
  partitioninfo1.set_end(5);

  partitioninfo2.set_fsid(fsID);
  partitioninfo2.set_poolid(2);
  partitioninfo2.set_copysetid(3);
  partitioninfo2.set_partitionid(4);
  partitioninfo2.set_start(5);
  partitioninfo2.set_end(6);

  pb::mds::topology::CreatePartitionResponse response;
  // 1. create partition return ok, but no partition info returns
  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));

  // 2. create partition ok
  response.add_partitioninfolist()->CopyFrom(partitioninfo1);
  response.add_partitioninfolist()->CopyFrom(partitioninfo2);

  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_TRUE(mdsclient_.CreatePartition(fsID, count, &out));
  ASSERT_EQ(2, out.size());
  ASSERT_TRUE(ComparePartition(out[0], partitioninfo1));
  ASSERT_TRUE(ComparePartition(out[1], partitioninfo2));

  // 3. create partition fail
  response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
      .WillOnce(SetArgPointee<2>(response));
  ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));

  // 4. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
      .WillRepeatedly(Invoke(CreatePartitionRpcFailed));
  ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));
}

TEST_F(MdsClientImplTest, ListPartition) {
  // in
  uint32_t fsID = 1;

  // out
  std::vector<PartitionInfo> out;

  PartitionInfo partitioninfo1;
  PartitionInfo partitioninfo2;
  partitioninfo1.set_fsid(fsID);
  partitioninfo1.set_poolid(1);
  partitioninfo1.set_copysetid(2);
  partitioninfo1.set_partitionid(3);
  partitioninfo1.set_start(4);
  partitioninfo1.set_end(5);

  partitioninfo2.set_fsid(fsID);
  partitioninfo2.set_poolid(2);
  partitioninfo2.set_copysetid(3);
  partitioninfo2.set_partitionid(4);
  partitioninfo2.set_start(5);
  partitioninfo2.set_end(6);

  pb::mds::topology::ListPartitionResponse response;
  response.add_partitioninfolist()->CopyFrom(partitioninfo1);
  response.add_partitioninfolist()->CopyFrom(partitioninfo2);

  // 1. get metaserver list in copysets ok
  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_TRUE(mdsclient_.ListPartition(fsID, &out));

  ASSERT_EQ(2, out.size());
  ASSERT_TRUE(ComparePartition(out[0], partitioninfo1));
  ASSERT_TRUE(ComparePartition(out[1], partitioninfo2));

  // 2. get metaserver list in copyset unknown error
  response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_FALSE(mdsclient_.ListPartition(fsID, &out));

  // 3. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
      .WillRepeatedly(Invoke(ListPartitionRpcFailed));
  ASSERT_FALSE(mdsclient_.ListPartition(fsID, &out));
}

TEST_F(MdsClientImplTest, GetCopysetOfPartition) {
  // in
  std::vector<uint32_t> partitionIDList{1, 2};

  // out
  std::map<uint32_t, Copyset> out;

  Copyset copyset1;
  Copyset copyset2;
  copyset1.set_poolid(1);
  copyset1.set_copysetid(2);
  Peer peer1;
  peer1.set_id(3);
  peer1.set_address("addr1");
  copyset1.add_peers()->CopyFrom(peer1);

  copyset2.set_poolid(2);
  copyset2.set_copysetid(3);
  Peer peer2;
  peer2.set_id(4);
  peer2.set_address("addr2");
  copyset2.add_peers()->CopyFrom(peer2);

  pb::mds::topology::GetCopysetOfPartitionResponse response;

  // 1. get metaserver list return ok, but no copyset returns
  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));

  // 2. get metaserver list in copysets ok
  auto copysetMap = response.mutable_copysetmap();
  (*copysetMap)[1] = copyset1;
  (*copysetMap)[2] = copyset2;

  response.set_statuscode(TopoStatusCode::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_TRUE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));
  ASSERT_TRUE(CompareCopyset(out[1], copyset1));
  ASSERT_TRUE(CompareCopyset(out[2], copyset2));

  // 3. get metaserver list in copyset unknown error
  response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));

  // 4. get rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
      .WillRepeatedly(Invoke(GetCopysetOfPartitionsRpcFailed));
  ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));
}

TEST_F(MdsClientImplTest, RefreshSession) {
  // prame in
  PartitionTxId tmp;
  tmp.set_partitionid(1);
  tmp.set_txid(2);
  std::vector<PartitionTxId> txIds({tmp});
  std::string fsName = "fs";
  Mountpoint mountpoint;
  mountpoint.set_hostname("127.0.0.1");
  mountpoint.set_port(9000);
  mountpoint.set_path("/mnt");

  // out
  std::vector<PartitionTxId> out;
  std::atomic<bool>* enableSumInDir = new std::atomic<bool>(true);
  pb::mds::RefreshSessionResponse response;

  {
    LOG(INFO) << "### case1: refresh session ok, no need update ###";
    response.set_statuscode(FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, RefreshSession(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_FALSE(mdsclient_.RefreshSession(txIds, &out, fsName, mountpoint));
    ASSERT_TRUE(out.empty());
  }

  {
    LOG(INFO) << "### case2: refresh session ok, need update ###";
    response.set_statuscode(FSStatusCode::OK);
    *response.mutable_latesttxidlist() = {txIds.begin(), txIds.end()};
    EXPECT_CALL(mockmdsbasecli_, RefreshSession(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_FALSE(mdsclient_.RefreshSession(txIds, &out, fsName, mountpoint));
    ASSERT_EQ(1, out.size());
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(out[0], tmp))
        << "out:\n"
        << out[0].ShortDebugString() << "tmp:\n"
        << tmp.ShortDebugString();
  }

  {
    LOG(INFO) << "### case3: rpc failed ###";
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, RefreshSession(_, _, _, _))
        .WillRepeatedly(Invoke(RefreshSessionRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR,
              mdsclient_.RefreshSession(txIds, &out, fsName, mountpoint));
  }
}

TEST_F(MdsClientImplTest, test_AllocOrGetMemcacheCluster) {
  pb::mds::topology::AllocOrGetMemcacheClusterResponse response;
  pb::mds::topology::MemcacheClusterInfo cluster1;
  cluster1.set_clusterid(1);
  pb::mds::topology::MemcacheServerInfo server;
  server.set_ip("127.0.0.1");
  server.set_port(1);
  *cluster1.add_servers() = server;
  response.set_allocated_cluster(
      new pb::mds::topology::MemcacheClusterInfo(cluster1));

  // 1. ok
  response.set_statuscode(pb::mds::topology::TOPO_OK);
  EXPECT_CALL(mockmdsbasecli_, AllocOrGetMemcacheCluster(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  pb::mds::topology::MemcacheClusterInfo cluster2;
  ASSERT_EQ(true, mdsclient_.AllocOrGetMemcacheCluster(1, &cluster2));

  // 2. no memcached
  response.set_statuscode(pb::mds::topology::TOPO_MEMCACHECLUSTER_NOT_FOUND);
  EXPECT_CALL(mockmdsbasecli_, AllocOrGetMemcacheCluster(_, _, _, _))
      .WillOnce(SetArgPointee<1>(response));
  ASSERT_EQ(false, mdsclient_.AllocOrGetMemcacheCluster(1, &cluster2));

  // 3. rpc error
  brpc::Controller cntl;
  cntl.SetFailed(ECONNRESET, "error connect reset");
  EXPECT_CALL(mockmdsbasecli_, AllocOrGetMemcacheCluster(_, _, _, _))
      .WillRepeatedly(Invoke(AllocOrGetMemcacheClusterRpcFailed));
  ASSERT_EQ(false, mdsclient_.AllocOrGetMemcacheCluster(1, &cluster2));
}

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs
