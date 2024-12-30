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

#include "dingofs/src/mdsv2/filesystem/dentry.h"

#include "bthread/mutex.h"

namespace dingofs {
namespace mdsv2 {

Dentry::Dentry(uint32_t fs_id, const std::string& name) : fs_id_(fs_id), name_(name) {
  bthread_mutex_init(&mutex_, nullptr);
}

Dentry::~Dentry() { bthread_mutex_destroy(&mutex_); }

std::string Dentry::SerializeAsString() {
  pb::mdsv2::Dentry dentry;
  dentry.set_fs_id(fs_id_);
  dentry.set_inode_id(ino_);
  dentry.set_parent_inode_id(parent_ino_);
  dentry.set_name(name_);
  dentry.set_type(type_);
  dentry.set_flag(flag_);

  return dentry.SerializeAsString();
}

}  // namespace mdsv2
}  // namespace dingofs