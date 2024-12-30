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

#include "dingofs/src/mdsv2/filesystem/inode.h"

#include "bthread/mutex.h"

namespace dingofs {
namespace mdsv2 {

Inode::Inode(uint32_t fs_id, uint64_t ino) : fs_id_(fs_id), ino_(ino) { bthread_mutex_init(&mutex_, nullptr); }

Inode::~Inode() { bthread_mutex_destroy(&mutex_); }

std::string Inode::SerializeAsString() {
  pb::mdsv2::Inode inode;
  inode.set_fs_id(fs_id_);
  inode.set_inode_id(ino_);
  inode.set_length(length_);
  inode.set_ctime(ctime_);
  inode.set_mtime(mtime_);
  inode.set_atime(atime_);
  inode.set_uid(uid_);
  inode.set_gid(gid_);
  inode.set_mode(mode_);
  inode.set_nlink(nlink_);
  inode.set_type(type_);
  inode.set_symlink(symlink_);
  inode.set_rdev(rdev_);
  inode.set_dtime(dtime_);
  inode.set_openmpcount(openmpcount_);

  return inode.SerializeAsString();
}

}  // namespace mdsv2
}  // namespace dingofs