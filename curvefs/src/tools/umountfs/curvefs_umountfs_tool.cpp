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
 * Project: curve
 * Created Date: 2021-09-14
 * Author: chengyi01
 */
#include "curvefs/src/tools/umountfs/curvefs_umountfs_tool.h"

DECLARE_string(fsname);
DECLARE_string(mountpoint);
DECLARE_string(confPath);
DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace umountfs {

void UmountfsTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsname=" << FLAGS_fsname
              << " -mountpoint=" << FLAGS_mountpoint
              << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int UmountfsTool::RunCommand() {
    // call system umount
    std::ostringstream cmd;
    std::string::size_type pos = FLAGS_mountpoint.find(':');
    if (pos == std::string::npos) {
        std::cerr << "mountpoint " << FLAGS_mountpoint << " is invalid.\n"
                  << std::endl;
        return -1;
    }
    std::string localPath(FLAGS_mountpoint.begin() + pos + 1,
                          FLAGS_mountpoint.end());
    cmd << "umount " << localPath;
    int ret = 0;
    try {
        std::string command = cmd.str();
        std::thread sysUmount(std::system, command.c_str());

        // umount from cluster
        ret = CurvefsToolRpc::RunCommand();
        sysUmount.join();
    } catch (std::exception& e) {
        std::cerr << "system umount " << localPath
                  << " failed, error info: " << e.what() << std::endl;
        ret = -1;
    }
    return ret;
}

int UmountfsTool::Init() {
    int ret = CurvefsToolRpc::Init();

    // adjust the unique element of the queue
    curvefs::mds::UmountFsRequest request;
    request.set_fsname(FLAGS_fsname);
    request.set_mountpoint(FLAGS_mountpoint);
    AddRequest(request);

    service_stub_func_ =
        std::bind(&curvefs::mds::MdsService_Stub::UmountFs, service_stub_.get(),
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3, nullptr);
    return ret;
}

void UmountfsTool::InitHostsAddr() {
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
}

void UmountfsTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

bool UmountfsTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        std::cerr << "umountfs " << FLAGS_mountpoint << " from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
    } else if (response_->statuscode() != curvefs::mds::FSStatusCode::OK) {
        std::cerr << "umount fs from mds: " << host << " fail, error code is "
                  << response_->statuscode() << "\n";
    } else {
        std::cout << "umount fs from cluster success.\n";
        ret = true;
    }
    return ret;
}

}  // namespace umountfs
}  // namespace tools
}  // namespace curvefs
