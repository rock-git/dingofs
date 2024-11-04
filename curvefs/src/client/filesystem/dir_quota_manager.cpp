// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/client/filesystem/dir_quota_manager.h"

#include <atomic>
#include <cstdint>
#include <unordered_map>

#include "curvefs/src/client/common/dynamic_config.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/common/define.h"
#include "glog/logging.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

USING_FLAG(flush_quota_interval_second);
USING_FLAG(load_quota_interval_second);

void DirQuotaManager::Start() {
  if (running_.load()) {
    return;
  }

  CURVEFS_ERROR rc = DoLoadQuotas();
  if (rc != CURVEFS_ERROR::OK) {
    LOG(FATAL) << "QuotaManager load quota failed, rc = " << rc;
    return;
  }

  timer_->Add([this] { FlushQuotas(); },
              FLAGS_flush_quota_interval_second * 1000);
  timer_->Add([this] { LoadQuotas(); },
              FLAGS_load_quota_interval_second * 1000);

  running_.store(true);
}

void DirQuotaManager::Stop() {
  if (!running_.load()) {
    return;
  }

  DoFlushQuotas();

  running_.store(false);
}

void DirQuotaManager::UpdateDirQuotaUsage(Ino ino, int64_t new_space,
                                          int64_t new_inodes) {
  Ino inode = ino;
  while (inode != ROOTINODEID) {
    // NOTE: now we should not enable recyble
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "UpdateDirUsage failed, ino = " << inode
                 << " is RECYCLEINODEID";
      return;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == CURVEFS_ERROR::OK) {
      dir_quota->UpdateUsage(new_space, new_inodes);
    }

    // recursively update its parent
    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "UpdateDirUsage failed, ino = " << inode
                 << " get parent failed, rc = " << rc;
      return;
    }

    inode = parent;
  }
}

bool DirQuotaManager::CheckDirQuota(Ino ino, int64_t space, int64_t inodes) {
  Ino inode = ino;

  while (inode != ROOTINODEID) {
    // NOTE: now we should not enable recyble
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "CheckDirQuota failed, inodeId=" << ino
                 << ", inodeId=" << inode << " is RECYCLEINODEID";
      return false;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == CURVEFS_ERROR::OK) {
      if (!dir_quota->CheckQuota(space, inodes)) {
        LOG(WARNING) << "CheckDirQuota failed, inodeId=" << ino
                     << ", check fail becase quota inodeId=" << inode;
        return false;
      }
    }

    // if inode no quota or quota is enough, check its parent
    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "CheckDirQuota failed, inodeId=" << ino
                 << " because get inodeId=" << inode
                 << " parent failed, rc = " << rc;
      return false;
    }

    inode = parent;
  }

  return true;
}

CURVEFS_ERROR DirQuotaManager::GetDirQuota(
    Ino ino, std::shared_ptr<DirQuota>& dir_quota) {
  ReadLockGuard lk(rwock_);
  auto iter = quotas_.find(ino);
  if (iter == quotas_.end()) {
    LOG(INFO) << "GetDirQuota failed, ino = " << ino << " not found in quotas_";
    return CURVEFS_ERROR::NOTEXIST;
  }

  dir_quota = iter->second;
  CHECK_NOTNULL(dir_quota);
  return CURVEFS_ERROR::OK;
}

void DirQuotaManager::FlushQuotas() {
  if (!running_.load()) {
    LOG(INFO) << "FlushQuotas is skipped, DirQuotaManager is not running";
    return;
  }

  DoFlushQuotas();

  timer_->Add([this] { FlushQuotas(); },
              FLAGS_flush_quota_interval_second * 1000);
}

void DirQuotaManager::DoFlushQuotas() {
  std::unordered_map<uint64_t, Usage> dir_usages;
  {
    ReadLockGuard lk(rwock_);
    for (const auto& [ino, dir_quota] : quotas_) {
      Usage usage = dir_quota->GetUsage();
      if (usage.bytes() == 0 && usage.inodes() == 0) {
        VLOG(3) << "DoFlushQuota skip ino: " << ino
                << " usage is 0, fs_id: " << fs_id_;
        continue;
      } else {
        dir_usages[ino] = usage;
      }
    }
  }

  if (dir_usages.empty()) {
    VLOG(3) << "DoFlushQuota is skipped dir_usages is empty, fs_id: " << fs_id_;
    return;
  }

  auto rc = meta_client_->FlushDirUsages(fs_id_, dir_usages);

  if (rc == MetaStatusCode::OK) {
    VLOG(6) << "FlushDirUsages success, fs_id: " << fs_id_;
    ReadLockGuard lk(rwock_);
    for (const auto& [ino, usage] : dir_usages) {
      auto iter = quotas_.find(ino);
      if (iter != quotas_.end()) {
        iter->second->UpdateUsage(-usage.bytes(), -usage.inodes());
      } else {
        LOG(INFO) << "FlushDirUsages success, but ino: " << ino
                  << " quota is removed  from quotas_";
      }
    }
  } else {
    LOG(WARNING) << "FlushFsUsage failed, fs_id: " << fs_id_ << ", rc: " << rc;
  }
}

void DirQuotaManager::LoadQuotas() {
  if (!running_.load()) {
    LOG(INFO) << "LoadQuotas is skipped, DirQuotaManager is not running";
    return;
  }

  DoLoadQuotas();

  timer_->Add([this] { LoadQuotas(); },
              FLAGS_load_quota_interval_second * 1000);
}

CURVEFS_ERROR DirQuotaManager::DoLoadQuotas() {
  std::unordered_map<uint64_t, Quota> loaded_dir_quotas;
  auto rc = meta_client_->LoadDirQuotas(fs_id_, loaded_dir_quotas);
  if (rc != MetaStatusCode::OK) {
    LOG(ERROR) << "LoadDirQuotas failed, fs_id: " << fs_id_ << ", rc: " << rc;
    return CURVEFS_ERROR::INTERNAL;
  } else {
    WriteLockGuard lk(rwock_);
    // Remove quotas that are not in loaded_dir_quotas
    for (auto iter = quotas_.begin(); iter != quotas_.end();) {
      if (loaded_dir_quotas.find(iter->first) == loaded_dir_quotas.end()) {
        iter = quotas_.erase(iter);
      } else {
        ++iter;
      }
    }

    // Update or add quotas from loaded_dir_quotas
    for (const auto& [ino, quota] : loaded_dir_quotas) {
      auto iter = quotas_.find(ino);
      if (iter != quotas_.end()) {
        iter->second->Refresh(quota);
      } else {
        quotas_[ino] = std::make_shared<DirQuota>(ino, quota);
      }
    }

    return CURVEFS_ERROR::OK;
  }
}

bool DirQuotaManager::HasDirQuota(Ino ino) {
  Ino inode = ino;
  while (inode != ROOTINODEID) {
    // NOTE: now we should not enable recyble
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "HasDirQuota failed, ino = " << inode
                 << " is RECYCLEINODEID";
      return false;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == CURVEFS_ERROR::OK) {
      return true;
    }

    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "HasDirQuota failed, ino = " << inode
                 << " get parent failed, rc = " << rc;
      return true;
    }

    inode = parent;
  }

  return false;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs