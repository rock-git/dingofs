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
 * @Project: dingo
 * @Date: 2021-12-15 10:53:45
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_PARTITION_CLEANER_H_
#define DINGOFS_SRC_METASERVER_PARTITION_CLEANER_H_

#include <memory>
#include <unordered_map>

#include "dingofs/mds.pb.h"
#include "metaserver/copyset/copyset_node.h"
#include "metaserver/partition.h"
#include "metaserver/s3/metaserver_s3_adaptor.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace metaserver {

class PartitionCleaner {
 public:
  explicit PartitionCleaner(const std::shared_ptr<Partition>& partition)
      : partition_(partition) {
    isStop_ = false;
    LOG(INFO) << "PartitionCleaner poolId = " << partition->GetPoolId()
              << ", partitionId = " << partition->GetPartitionId();
  }

  void SetIndoDeletePeriod(uint32_t periodMs) {
    inodeDeletePeriodMs_ = periodMs;
  }

  void SetS3Aapter(std::shared_ptr<S3ClientAdaptor> s3Adaptor) {
    s3Adaptor_ = s3Adaptor;
  }

  void SetCopysetNode(copyset::CopysetNode* copysetNode) {
    copysetNode_ = copysetNode;
  }

  void SetMdsClient(std::shared_ptr<stub::rpcclient::MdsClient> mdsClient) {
    mdsClient_ = mdsClient;
  }

  bool ScanPartition();
  pb::metaserver::MetaStatusCode CleanDataAndDeleteInode(
      const pb::metaserver::Inode& inode);
  pb::metaserver::MetaStatusCode DeleteInode(
      const pb::metaserver::Inode& inode);
  pb::metaserver::MetaStatusCode DeletePartition();
  uint32_t GetPartitionId() { return partition_->GetPartitionId(); }

  void Stop() { isStop_ = true; }

  bool IsStop() { return isStop_; }

 private:
  std::shared_ptr<Partition> partition_;
  copyset::CopysetNode* copysetNode_;
  std::shared_ptr<S3ClientAdaptor> s3Adaptor_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;
  bool isStop_;
  uint32_t inodeDeletePeriodMs_;
  std::unordered_map<uint32_t, pb::mds::FsInfo> fsInfoMap_;
};

class PartitionCleanerClosure : public google::protobuf::Closure {
 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  bool runned_ = false;

 public:
  void Run() override {
    std::lock_guard<std::mutex> l(mutex_);
    runned_ = true;
    cond_.notify_one();
  }

  void WaitRunned() {
    std::unique_lock<std::mutex> ul(mutex_);
    cond_.wait(ul, [this]() { return runned_; });
  }
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_PARTITION_CLEANER_H_
