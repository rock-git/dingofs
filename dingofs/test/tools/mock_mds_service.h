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
 * @Date: 2021-09-27
 * @Author: chengyi01
 */

#ifndef DINGOFS_TEST_TOOLS_MOCK_MDS_SERVICE_H_
#define DINGOFS_TEST_TOOLS_MOCK_MDS_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/proto/mds.pb.h"

namespace dingofs {
namespace tools {

class MockMdsService : public dingofs::mds::MdsService {
 public:
  MockMdsService() : MdsService() {}
  ~MockMdsService() = default;

  MOCK_METHOD4(CreateFs, void(::google::protobuf::RpcController* controller,
                              const ::dingofs::mds::CreateFsRequest* request,
                              ::dingofs::mds::CreateFsResponse* response,
                              ::google::protobuf::Closure* done));
  MOCK_METHOD4(MountFs, void(::google::protobuf::RpcController* controller,
                             const ::dingofs::mds::MountFsRequest* request,
                             ::dingofs::mds::MountFsResponse* response,
                             ::google::protobuf::Closure* done));
  MOCK_METHOD4(UmountFs, void(::google::protobuf::RpcController* controller,
                              const ::dingofs::mds::UmountFsRequest* request,
                              ::dingofs::mds::UmountFsResponse* response,
                              ::google::protobuf::Closure* done));
  MOCK_METHOD4(GetFsInfo, void(::google::protobuf::RpcController* controller,
                               const ::dingofs::mds::GetFsInfoRequest* request,
                               ::dingofs::mds::GetFsInfoResponse* response,
                               ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeleteFs, void(::google::protobuf::RpcController* controller,
                              const ::dingofs::mds::DeleteFsRequest* request,
                              ::dingofs::mds::DeleteFsResponse* response,
                              ::google::protobuf::Closure* done));
};
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_TEST_TOOLS_MOCK_MDS_SERVICE_H_