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

#ifndef DINGOFS_MDV2_FILESYSTEM_INODE_H_
#define DINGOFS_MDV2_FILESYSTEM_INODE_H_

#include <cstdint>
#include <map>
#include <memory>

#include "bthread/types.h"
#include "dingofs/proto/mdsv2.pb.h"

namespace dingofs {
namespace mdsv2 {

class Inode;
using InodePtr = std::shared_ptr<Inode>;

class Inode {
 public:
  Inode(uint32_t fs_id, uint64_t ino);
  ~Inode();

  Inode(const Inode&) = delete;
  Inode operator=(const Inode&) = delete;

  static InodePtr New(uint32_t fs_id, uint64_t ino) { return std::make_shared<Inode>(fs_id, ino); }

  uint32_t GetFsId() const { return fs_id_; }
  uint64_t GetIno() const { return ino_; }

  uint64_t GetLength() const { return length_; }
  void SetLength(uint64_t length) { length_ = length; }

  uint64_t GetCtime() const { return ctime_; }
  void SetCtime(uint64_t ctime) { ctime_ = ctime; }

  uint64_t GetMtime() const { return mtime_; }
  void SetMtime(uint64_t mtime) { mtime_ = mtime; }

  uint64_t GetAtime() const { return atime_; }
  void SetAtime(uint64_t atime) { atime_ = atime; }

  uint32_t GetUid() const { return uid_; }
  void SetUid(uint32_t uid) { uid_ = uid; }

  uint32_t GetGid() const { return gid_; }
  void SetGid(uint32_t gid) { gid_ = gid; }

  uint32_t GetMode() const { return mode_; }
  void SetMode(uint32_t mode) { mode_ = mode; }

  uint32_t GetNlink() const { return nlink_; }
  void SetNlink(uint32_t nlink) { nlink_ = nlink; }

  pb::mdsv2::FileType GetType() const { return type_; }
  void SetType(pb::mdsv2::FileType type) { type_ = type; }

  const std::string& GetSymlink() const { return symlink_; }
  void SetSymlink(const std::string& symlink) { symlink_ = symlink; }

  uint64_t GetRdev() const { return rdev_; }
  void SetRdev(uint64_t rdev) { rdev_ = rdev; }

  uint32_t GetDtime() const { return dtime_; }
  void SetDtime(uint32_t dtime) { dtime_ = dtime; }

  uint32_t GetOpenmpcount() const { return openmpcount_; }
  void SetOpenmpcount(uint32_t openmpcount) { openmpcount_ = openmpcount; }

  std::string SerializeAsString();

 private:
  bthread_mutex_t mutex_;

  uint32_t fs_id_{0};
  uint64_t ino_{0};
  uint64_t length_{0};
  uint64_t ctime_{0};
  uint64_t mtime_{0};
  uint64_t atime_{0};
  uint32_t uid_{0};
  uint32_t gid_{0};
  uint32_t mode_{0};
  uint32_t nlink_{0};
  pb::mdsv2::FileType type_{0};
  std::string symlink_;
  uint64_t rdev_{0};
  uint32_t dtime_{0};
  uint32_t openmpcount_{0};
  std::map<uint64_t, pb::mdsv2::S3ChunkList> s3_chunk_map_;
  std::map<std::string, std::string> xattr_map_;
};

class InodeMap {
 public:
  InodeMap() = default;
  ~InodeMap() = default;

  InodePtr GetInode(uint64_t inode_id) { return inode_map_[inode_id]; }
  void AddInode(uint64_t inode_id, InodePtr inode){};
  void RemoveInode(uint64_t inode_id){};

 private:
  bthread_mutex_t mutex_;

  std::map<uint64_t, InodePtr> inode_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_INODE_H_