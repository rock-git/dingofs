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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/fuse_s3_client.h"

#include <memory>

#include "curvefs/src/client/blockcache/block_cache.h"
#include "curvefs/src/client/blockcache/s3_client.h"
#include "curvefs/src/client/datastream/data_stream.h"
#include "curvefs/src/stub/filesystem/xattr.h"
#include "curvefs/src/client/kvclient/memcache_client.h"
#include "curvefs/src/utils/net_common.h"

namespace curvefs {
namespace client {
namespace common {

DECLARE_bool(enableCto);
DECLARE_bool(supportKVcache);

}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using ::curvefs::base::string::StrFormat;
using ::curvefs::client::blockcache::BlockCacheImpl;
using ::curvefs::client::blockcache::S3ClientImpl;
using curvefs::client::common::FLAGS_enableCto;
using curvefs::client::common::FLAGS_supportKVcache;
using ::curvefs::client::datastream::DataStream;
using curvefs::mds::topology::MemcacheClusterInfo;

CURVEFS_ERROR FuseS3Client::Init(const FuseClientOption& option) {
  FuseClientOption opt(option);

  CURVEFS_ERROR ret = FuseClient::Init(opt);
  if (ret != CURVEFS_ERROR::OK) {
    return ret;
  }

  ret = InitBrpcServer();
  if (ret != CURVEFS_ERROR::OK) {
    return ret;
  }

  // init kvcache
  if (FLAGS_supportKVcache && !InitKVCache(option.kvClientManagerOpt)) {
    return CURVEFS_ERROR::INTERNAL;
  }

  // set fs S3Option
  const auto& s3Info = fsInfo_->detail().s3info();
  ::curvefs::aws::S3InfoOption fsS3Option;
  ::curvefs::client::common::S3Info2FsS3Option(s3Info, &fsS3Option);
  SetFuseClientS3Option(&opt, fsS3Option);

  S3ClientImpl::GetInstance()->Init(opt.s3Opt.s3AdaptrOpt);

  auto page_option = option.data_stream_option.page_option;
  auto max_memory_size = page_option.total_size;
  auto fsCacheManager = std::make_shared<FsCacheManager>(
      dynamic_cast<S3ClientAdaptorImpl*>(s3Adaptor_.get()),
      opt.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte, max_memory_size,
      opt.s3Opt.s3ClientAdaptorOpt.readCacheThreads, kvClientManager_);

  // data stream
  if (!DataStream::GetInstance().Init(option.data_stream_option)) {
    return CURVEFS_ERROR::INTERNAL;
  }

  // block cache
  auto block_cache_option = option.block_cache_option;
  std::string uuid = StrFormat("%d-%s", fsInfo_->fsid(), fsInfo_->fsname());
  if (fsInfo_->has_uuid()) {
    uuid = fsInfo_->uuid();
  }
  RewriteCacheDir(&block_cache_option, uuid);
  auto block_cache = std::make_shared<BlockCacheImpl>(block_cache_option);

  return s3Adaptor_->Init(opt.s3Opt.s3ClientAdaptorOpt,
                          S3ClientImpl::GetInstance(), inodeManager_,
                          mdsClient_, fsCacheManager, GetFileSystem(),
                          block_cache, kvClientManager_, true);
}

bool FuseS3Client::InitKVCache(const KVClientManagerOpt& opt) {
  // get kvcache cluster
  MemcacheClusterInfo kvcachecluster;
  if (!mdsClient_->AllocOrGetMemcacheCluster(fsInfo_->fsid(),
                                             &kvcachecluster)) {
    LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
               << ", but AllocOrGetMemcacheCluster fail";
    return false;
  }

  // init kvcache client
  auto memcacheClient = std::make_shared<MemCachedClient>();
  if (!memcacheClient->Init(kvcachecluster)) {
    LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
               << ", but init memcache client fail";
    return false;
  }

  kvClientManager_ = std::make_shared<KVClientManager>();
  if (!kvClientManager_->Init(opt, memcacheClient)) {
    LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
               << ", but init kvClientManager fail";
    return false;
  }

  if (warmupManager_ != nullptr) {
    warmupManager_->SetKVClientManager(kvClientManager_);
  }

  return true;
}

void FuseS3Client::UnInit() {
  FuseClient::UnInit();
  s3Adaptor_->Stop();
  S3ClientImpl::GetInstance()->Destroy();
  DataStream::GetInstance().Shutdown();
  curvefs::aws::S3Adapter::Shutdown();
}

CURVEFS_ERROR FuseS3Client::FuseOpInit(void* userdata,
                                       struct fuse_conn_info* conn) {
  CURVEFS_ERROR ret = FuseClient::FuseOpInit(userdata, conn);
  if (init_) {
    s3Adaptor_->SetFsId(fsInfo_->fsid());
  }
  return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
                                        const char* buf, size_t size, off_t off,
                                        struct fuse_file_info* fi,
                                        FileOut* file_out) {
  size_t* w_size = &file_out->nwritten;
  // check align
  if (fi->flags & O_DIRECT) {
    if (!(is_aligned(off, DirectIOAlignment) &&
          is_aligned(size, DirectIOAlignment)))
      return CURVEFS_ERROR::INVALIDPARAM;
  }

  if (!fs_->CheckQuota(ino, size, 0)) {
    return CURVEFS_ERROR::NO_SPACE;
  }

  uint64_t start = butil::cpuwide_time_us();
  int w_ret = s3Adaptor_->Write(ino, off, size, buf);
  if (w_ret < 0) {
    LOG(ERROR) << "s3Adaptor_ write failed, ret = " << w_ret;
    return CURVEFS_ERROR::INTERNAL;
  }

  if (fsMetric_ != nullptr) {
    fsMetric_->userWrite.bps.count << w_ret;
    fsMetric_->userWrite.qps.count << 1;
    uint64_t duration = butil::cpuwide_time_us() - start;
    fsMetric_->userWrite.latency << duration;
    fsMetric_->userWriteIoSize.set_value(w_ret);
  }

  std::shared_ptr<InodeWrapper> inode_wrapper;
  CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  // TODO:  maybe we should check ret is ok
  if (ret != CURVEFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  size_t change_size = 0;

  {
    ::curvefs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();

    *w_size = w_ret;
    // update file len
    if (inode_wrapper->GetLengthLocked() < off + *w_size) {
      change_size = off + *w_size - inode_wrapper->GetLengthLocked();
      inode_wrapper->SetLengthLocked(off + *w_size);
    }

    inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    inodeManager_->ShipToFlush(inode_wrapper);

    if (fi->flags & O_DIRECT || fi->flags & O_SYNC || fi->flags & O_DSYNC) {
      // Todo: do some cache flush later
    }

    inode_wrapper->GetInodeAttrUnLocked(&file_out->attr);
  }

  for (int i = 0; i < file_out->attr.parent_size(); i++) {
    auto parent = file_out->attr.parent(i);
    fs_->UpdateDirQuotaUsage(parent, change_size, 0);
  }
  fs_->UpdateFsQuotaUsage(change_size, 0);

  return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req, fuse_ino_t ino,
                                       size_t size, off_t off,
                                       struct fuse_file_info* fi, char* buffer,
                                       size_t* r_size) {
  (void)req;
  auto GetReadSize = [](size_t& size, off_t& off, size_t& file_size) -> size_t {
    if (static_cast<int64_t>(file_size) <= off) {
      return 0;
    } else if (file_size < off + size) {
      return file_size - off;
    } else {
      return size;
    }
  };

  if (ino == STATSINODEID) {
    auto handler = fs_->FindHandler(fi->fh);
    auto* data_buf = handler->buffer;

    size_t file_size = data_buf->size;
    size_t len = GetReadSize(size, off, file_size);
    *r_size = len;
    if (len > 0) {
      memcpy(buffer, data_buf->p + off, len);
    }
    return CURVEFS_ERROR::OK;
  }

  // check align
  if (fi->flags & O_DIRECT) {
    if (!(is_aligned(off, DirectIOAlignment) &&
          is_aligned(size, DirectIOAlignment)))
      return CURVEFS_ERROR::INVALIDPARAM;
  }

  uint64_t start = butil::cpuwide_time_us();
  std::shared_ptr<InodeWrapper> inode_wrapper;
  CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  if (ret != CURVEFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }
  uint64_t file_size = inode_wrapper->GetLength();

  size_t len = GetReadSize(size, off, file_size);
  if (len == 0) {
    *r_size = 0;
    return CURVEFS_ERROR::OK;
  }

  // Read do not change inode. so we do not get lock here.
  int r_ret = s3Adaptor_->Read(ino, off, len, buffer);
  if (r_ret < 0) {
    LOG(ERROR) << "s3Adaptor_ read failed, ret = " << r_ret;
    return CURVEFS_ERROR::INTERNAL;
  }
  *r_size = r_ret;

  if (fsMetric_.get() != nullptr) {
    fsMetric_->userRead.bps.count << r_ret;
    fsMetric_->userRead.qps.count << 1;
    uint64_t duration = butil::cpuwide_time_us() - start;
    fsMetric_->userRead.latency << duration;
    fsMetric_->userReadIoSize.set_value(r_ret);
  }

  ::curvefs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
  inode_wrapper->UpdateTimestampLocked(kAccessTime);
  inodeManager_->ShipToFlush(inode_wrapper);

  VLOG(9) << "read end, read size = " << *r_size;
  return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                         const char* name, mode_t mode,
                                         struct fuse_file_info* fi,
                                         EntryOut* entry_out) {
  VLOG(1) << "FuseOpCreate, parent: " << parent << ", name: " << name
          << ", mode: " << mode;

  std::shared_ptr<InodeWrapper> inode;
  CURVEFS_ERROR ret =
      MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, 0, false, inode);
  if (ret != CURVEFS_ERROR::OK) {
    return ret;
  }

  auto openFiles = fs_->BorrowMember().openFiles;
  openFiles->Open(inode->GetInodeId(), inode);

  inode->GetInodeAttr(&entry_out->attr);

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Remeber(entry_out->attr, name);

  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                        const char* name, mode_t mode,
                                        dev_t rdev, EntryOut* entry_out) {
  VLOG(1) << "FuseOpMkNod, parent: " << parent << ", name: " << name
          << ", mode: " << mode << ", rdev: " << rdev;

  std::shared_ptr<InodeWrapper> inode;
  CURVEFS_ERROR rc = MakeNode(req, parent, name, mode, FsFileType::TYPE_S3,
                              rdev, false, inode);
  if (rc != CURVEFS_ERROR::OK) {
    return rc;
  }

  InodeAttr attr;
  inode->GetInodeAttr(&attr);
  *entry_out = EntryOut(attr);

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Remeber(attr, name);

  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                       fuse_ino_t newparent,
                                       const char* newname,
                                       EntryOut* entry_out) {
  VLOG(1) << "FuseOpLink, inodeId=" << ino << ", newparent: " << newparent
          << ", newname: " << newname;
  return FuseClient::OpLink(req, ino, newparent, newname, FsFileType::TYPE_S3,
                            entry_out);
}

CURVEFS_ERROR FuseS3Client::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                         const char* name) {
  VLOG(1) << "FuseOpUnlink, parent: " << parent << ", name: " << name;
  return FuseClient::OpUnlink(req, parent, name, FsFileType::TYPE_S3);
}

CURVEFS_ERROR FuseS3Client::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                        int datasync,
                                        struct fuse_file_info* fi) {
  (void)req;
  (void)fi;
  VLOG(1) << "FuseOpFsync, inodeId=" << ino << ", datasync: " << datasync;

  CURVEFS_ERROR ret = s3Adaptor_->Flush(ino);
  if (ret != CURVEFS_ERROR::OK) {
    LOG(ERROR) << "s3Adaptor_ flush failed, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }
  if (datasync != 0) {
    return CURVEFS_ERROR::OK;
  }
  std::shared_ptr<InodeWrapper> inodeWrapper;
  ret = inodeManager_->GetInode(ino, inodeWrapper);
  if (ret != CURVEFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }
  ::curvefs::utils::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
  return inodeWrapper->Sync();
}

// NOTE: inode lock should be acqurire before calling this function
CURVEFS_ERROR FuseS3Client::Truncate(InodeWrapper* inode, uint64_t length) {
  InodeAttr attr;
  inode->GetInodeAttrUnLocked(&attr);
  int64_t change_size = length - static_cast<int64_t>(attr.length());

  if (change_size > 0) {
    if (!fs_->CheckFsQuota(change_size, 0)) {
      return CURVEFS_ERROR::NO_SPACE;
    }

    for (int i = 0; i < attr.parent_size(); i++) {
      auto parent = attr.parent(i);
      if (!fs_->CheckDirQuota(parent, change_size, 0)) {
        return CURVEFS_ERROR::NO_SPACE;
      }
    }
  }

  CURVEFS_ERROR rc = s3Adaptor_->Truncate(inode, length);

  if (rc == CURVEFS_ERROR::OK) {
    for (int i = 0; i < attr.parent_size(); i++) {
      auto parent = attr.parent(i);
      fs_->UpdateDirQuotaUsage(parent, change_size, 0);
    }

    fs_->UpdateFsQuotaUsage(change_size, 0);
  }

  return rc;
}

CURVEFS_ERROR FuseS3Client::FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi) {
  (void)req;
  (void)fi;
  VLOG(1) << "FuseOpFlush, inodeId=" << ino;
  CURVEFS_ERROR ret = CURVEFS_ERROR::OK;

  if (ino == STATSINODEID) return ret;

  auto entry_watcher = fs_->BorrowMember().entry_watcher;

  // if enableCto, flush all write cache both in memory cache and disk cache
  if (FLAGS_enableCto && !entry_watcher->ShouldWriteback(ino)) {
    ret = s3Adaptor_->FlushAllCache(ino);
    if (ret != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "FuseOpFlush, flush all cache fail, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }
    VLOG(3) << "FuseOpFlush, flush to s3 ok, inodeId=" << ino;

    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "FuseOpFlush, inodeManager get inode fail, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }

    ::curvefs::utils::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    ret = inodeWrapper->Sync();
    if (ret != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "FuseOpFlush, inode sync s3 chunk info fail, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }
    // if disableCto, flush just flush data in memory
  } else {
    ret = s3Adaptor_->Flush(ino);
    if (ret != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "FuseOpFlush, flush to diskcache failed, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }
  }

  VLOG(1) << "FuseOpFlush, inodeId=" << ino << " flush ok";
  return CURVEFS_ERROR::OK;
}

void FuseS3Client::FlushData() {
  CURVEFS_ERROR ret = CURVEFS_ERROR::UNKNOWN;
  do {
    ret = s3Adaptor_->FsSync();
  } while (ret != CURVEFS_ERROR::OK);
}

static bool StartBrpcServer(brpc::Server& server, brpc::ServerOptions* options,
                            uint32_t start_port, uint32_t end_port,
                            uint32_t* listen_port) {
  static std::once_flag flag;
  std::call_once(flag, [&]() {
    while (start_port < end_port) {
      if (server.Start(start_port, options) == 0) {
        LOG(INFO) << "Start brpc server success, listen port = " << start_port;
        *listen_port = start_port;
        break;
      }

      ++start_port;
    }
  });

  if (start_port >= end_port) {
    LOG(ERROR) << "Start brpc server failed, start_port = " << start_port;
    return false;
  }

  return true;
}

CURVEFS_ERROR FuseS3Client::InitBrpcServer() {
  inode_object_service_.Init(s3Adaptor_, inodeManager_);

  if (server_.AddService(&inode_object_service_,
                         brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add InodeObjectsService";
    return CURVEFS_ERROR::INTERNAL;
  }

  brpc::ServerOptions brpc_server_options;

  uint32_t listen_port = 0;
  if (!StartBrpcServer(server_, &brpc_server_options,
                       option_.dummyServerStartPort, PORT_LIMIT,
                       &listen_port)) {
    return CURVEFS_ERROR::INTERNAL;
  }

  std::string local_ip;
  if (!curvefs::utils::NetCommon::GetLocalIP(&local_ip)) {
    LOG(ERROR) << "Get local ip failed!";
    return CURVEFS_ERROR::INTERNAL;
  }

  curvefs::stub::common::ClientDummyServerInfo::GetInstance().SetPort(listen_port);
  curvefs::stub::common::ClientDummyServerInfo::GetInstance().SetIP(local_ip);

  return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
