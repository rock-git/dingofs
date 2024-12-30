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

#ifndef DINGOFS_MDV2_FILESYSTEM_H_
#define DINGOFS_MDV2_FILESYSTEM_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/common/status.h"
#include "dingofs/src/mdsv2/filesystem/dentry.h"
#include "dingofs/src/mdsv2/filesystem/id_generator.h"
#include "dingofs/src/mdsv2/filesystem/inode.h"
#include "dingofs/src/mdsv2/storage/storage.h"

namespace dingofs {

namespace mdsv2 {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

class FileSystemSet;
using FileSystemSetPtr = std::shared_ptr<FileSystemSet>;

class FileSystem {
 public:
  FileSystem(KVStoragePtr kv_storage) : kv_storage_(kv_storage){};
  ~FileSystem() = default;

  static FileSystemPtr New(KVStoragePtr kv_storage) { return std::make_shared<FileSystem>(kv_storage); }

  bool Init();

  // dentry/inode
  Status MkNod(const pb::mdsv2::MkNodRequest* request);
  Status MkDir(const pb::mdsv2::MkDirRequest* request);
  Status RmDir(const pb::mdsv2::RmDirRequest* request);

 private:
  Status GenIno(int64_t& ino);

  IdGeneratorPtr id_generator_;
  KVStoragePtr kv_storage_;

  DentryMap dentry_map_;
  InodeMap inode_map_;
};

// manage all filesystem
class FileSystemSet {
 public:
  FileSystemSet(KVStorage* kv_storage);
  ~FileSystemSet();

  static FileSystemSetPtr New(KVStorage* kv_storage) { return std::make_shared<FileSystemSet>(kv_storage); }

  bool Init();

  Status CreateFs(const pb::mdsv2::FsInfo& fs_info);
  Status MountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name);
  Status GetFsInfo(const std::string& fs_name, pb::mdsv2::FsInfo& fs_info);

 private:
  Status CreateFsTable();
  bool IsExistFsTable();

  KVStorage* kv_storage_{nullptr};

  bthread_mutex_t mutex_;
  // key: fs_id
  std::map<uint32_t, FileSystemPtr> fs_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_