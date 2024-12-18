/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: curve
 * Created Date: 2022-05-25
 * Author: wanghai01
 */
#ifndef CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_PARTITION_TOOL_H_
#define CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_PARTITION_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/utils/string_util.h"

namespace curvefs {
namespace tools {
namespace delete_ {

class DeletePartitionTool
    : public CurvefsToolRpc<curvefs::mds::topology::DeletePartitionRequest,
                            curvefs::mds::topology::DeletePartitionResponse,
                            curvefs::mds::topology::TopologyService_Stub> {
 public:
  explicit DeletePartitionTool(const std::string& cmd = kPartitionDeleteCmd)
      : CurvefsToolRpc(cmd) {}

  void PrintHelp() override;
  int Init() override;
  void InitHostsAddr() override;
  int RunCommand() override;

 protected:
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;
  bool CheckRequiredFlagDefault() override;

 protected:
};

}  // namespace delete_
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_PARTITION_TOOL_H_
