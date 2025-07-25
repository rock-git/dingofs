/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Dingofs
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_

#include <memory>
#include <vector>

#include "absl/container/btree_map.h"
#include "client/vfs_legacy/filesystem/meta.h"
#include "metrics/client/vfs_legacy/dir_cache.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "utils/concurrent/concurrent.h"
#include "utils/lru_cache.h"
#include "utils/message_queue.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace filesystem {

class DirEntryList {
 public:
  using IterateHandler = std::function<void(DirEntry* dirEntry)>;

  DirEntryList();

  size_t Size();

  void Add(const DirEntry& dirEntry);

  void Iterate(IterateHandler handler);

  bool Get(Ino ino, DirEntry* dirEntry);

  bool UpdateAttr(Ino ino, const pb::metaserver::InodeAttr& attr);

  bool UpdateLength(Ino ino, const pb::metaserver::InodeAttr& open);

  void Clear();

  void SetMtime(utils::TimeSpec mtime);

  utils::TimeSpec GetMtime();

 private:
  utils::RWLock rwlock_;
  utils::TimeSpec mtime_;
  std::vector<DirEntry> entries_;
  absl::btree_map<Ino, uint32_t> index_;
};

class DirCache {
 public:
  using LRUType = utils::LRUCache<Ino, std::shared_ptr<DirEntryList>>;
  using MessageType = std::shared_ptr<DirEntryList>;
  using MessageQueueType = utils::MessageQueue<MessageType>;

  explicit DirCache(DirCacheOption option);

  void Start();

  void Stop();

  void Put(Ino parent, std::shared_ptr<DirEntryList> entries);

  bool Get(Ino parent, std::shared_ptr<DirEntryList>* entries);

  void Drop(Ino parent);

 private:
  void Delete(Ino parent, std::shared_ptr<DirEntryList> entries, bool evit);

  void Evit(size_t size);

  utils::RWLock rwlock_;
  size_t nentries_;
  DirCacheOption option_;
  std::shared_ptr<LRUType> lru_;
  std::shared_ptr<MessageQueueType> mq_;
  std::shared_ptr<metrics::client::vfs_legacy::DirCacheMetric> metric_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_
