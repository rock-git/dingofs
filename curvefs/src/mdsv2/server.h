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

#ifndef DINGOFS_MDSV2_SERVER_H_
#define DINGOFS_MDSV2_SERVER_H_

#include <memory>
#include <string>

#include "curvefs/src/mdsv2/common/crontab.h"
#include "curvefs/src/mdsv2/coordinator/coordinator_client.h"
#include "curvefs/src/mdsv2/filesystem/filesystem.h"
#include "curvefs/src/mdsv2/service/heartbeat.h"
#include "curvefs/src/mdsv2/service/mds_meta.h"
#include "curvefs/src/mdsv2/storage/storage.h"
#include "curvefs/src/utils/configuration.h"

namespace dingofs {

namespace mdsv2 {

using ::curvefs::utils::Configuration;

class Server {
 public:
  static Server& GetInstance();

  bool InitConfig(const std::string& path);

  bool InitLog();

  bool InitMDSMeta();

  bool InitCoordinatorClient(const std::string& coor_url);

  bool InitFsIdGenerator();

  bool InitStorage(const std::string& store_url);

  bool InitFileSystem();

  bool InitHeartbeat();

  bool InitWorkerSet();

  bool InitCrontab();

  std::string GetListenAddr();
  MDSMeta& GetMDSMeta();
  Heartbeat& GetHeartbeat() { return heartbeat_; }
  CoordinatorClient& GetCoordinatorClient() { return coordinator_client_; }

  void Run();

  void Stop();

 private:
  explicit Server() = default;
  ~Server();

  Configuration conf_;

  // mds self info
  MDSMeta mds_meta_;

  // This is manage crontab, like heartbeat.
  CrontabManager crontab_manager_;
  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;

  // coordinator client
  CoordinatorClient coordinator_client_;

  // fs id generator
  IdGeneratorPtr fs_id_generator_;

  // backend kv storage
  KVStorage* kv_storage_{nullptr};

  // filesystem
  FileSystemPtr file_system_;

  // heartbeat to coordinator
  Heartbeat heartbeat_;

  // worker set for service request
  WorkerSetPtr read_worker_set_;
  WorkerSetPtr write_worker_set_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVER_H_