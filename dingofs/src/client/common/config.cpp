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
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "dingofs/src/client/common/config.h"

#include <gflags/gflags.h>

#include <string>
#include <vector>

#include "dingofs/src/base/filepath/filepath.h"
#include "dingofs/src/base/math/math.h"
#include "dingofs/src/base/string/string.h"
#include "dingofs/src/client/common/dynamic_config.h"
#include "dingofs/src/utils/gflags_helper.h"
#include "dingofs/src/utils/string_util.h"

namespace brpc {
DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace dingofs {
namespace client {
namespace common {
DECLARE_bool(useFakeS3);
}  // namespace common
}  // namespace client
}  // namespace dingofs

namespace dingofs {
namespace client {
namespace common {

using dingofs::aws::S3InfoOption;
using ::dingofs::base::filepath::PathJoin;
using ::dingofs::base::math::kMiB;
using ::dingofs::base::string::Str2Int;
using dingofs::utils::Configuration;

using ::dingofs::base::string::StrSplit;
using dingofs::stub::common::ExcutorOpt;
using dingofs::stub::common::MetaCacheOpt;

static bool pass_bool(const char*, bool) { return true; }
DEFINE_bool(enableCto, true, "acheieve cto consistency");
DEFINE_bool(useFakeS3, false,
            "Use fake s3 to inject more metadata for testing metaserver");
DEFINE_bool(supportKVcache, false, "use kvcache to speed up sharing");
DEFINE_bool(access_logging, true, "enable access log");
DEFINE_validator(access_logging, &pass_bool);

/**
 * use curl -L fuseclient:port/flags/fuseClientAvgWriteBytes?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_uint64(const char*, uint64_t) { return true; }

DEFINE_uint64(fuseClientAvgWriteBytes, 0,
              "the write throttle bps of fuse client");
DEFINE_validator(fuseClientAvgWriteBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteBytes, 0,
              "the write burst bps of fuse client");
DEFINE_validator(fuseClientBurstWriteBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteBytesSecs, 180,
              "the times that write burst bps can continue");
DEFINE_validator(fuseClientBurstWriteBytesSecs, &pass_uint64);

DEFINE_uint64(fuseClientAvgWriteIops, 0,
              "the write throttle iops of fuse client");
DEFINE_validator(fuseClientAvgWriteIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteIops, 0,
              "the write burst iops of fuse client");
DEFINE_validator(fuseClientBurstWriteIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteIopsSecs, 180,
              "the times that write burst iops can continue");
DEFINE_validator(fuseClientBurstWriteIopsSecs, &pass_uint64);

DEFINE_uint64(fuseClientAvgReadBytes, 0,
              "the Read throttle bps of fuse client");
DEFINE_validator(fuseClientAvgReadBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadBytes, 0, "the Read burst bps of fuse client");
DEFINE_validator(fuseClientBurstReadBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadBytesSecs, 180,
              "the times that Read burst bps can continue");
DEFINE_validator(fuseClientBurstReadBytesSecs, &pass_uint64);

DEFINE_uint64(fuseClientAvgReadIops, 0,
              "the Read throttle iops of fuse client");
DEFINE_validator(fuseClientAvgReadIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadIops, 0, "the Read burst iops of fuse client");
DEFINE_validator(fuseClientBurstReadIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadIopsSecs, 180,
              "the times that Read burst iops can continue");
DEFINE_validator(fuseClientBurstReadIopsSecs, &pass_uint64);

void InitMetaCacheOption(Configuration* conf, MetaCacheOpt* opts) {
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                            &opts->metacacheGetLeaderRetry);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                            &opts->metacacheRPCRetryIntervalUS);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                            &opts->metacacheGetLeaderRPCTimeOutMS);
}

void InitExcutorOption(Configuration* conf, ExcutorOpt* opts, bool internal) {
  if (internal) {
    conf->GetValueFatalIfFail("executorOpt.maxInternalRetry", &opts->maxRetry);
  } else {
    conf->GetValueFatalIfFail("executorOpt.maxRetry", &opts->maxRetry);
  }

  conf->GetValueFatalIfFail("executorOpt.retryIntervalUS",
                            &opts->retryIntervalUS);
  conf->GetValueFatalIfFail("executorOpt.rpcTimeoutMS", &opts->rpcTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.rpcStreamIdleTimeoutMS",
                            &opts->rpcStreamIdleTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.maxRPCTimeoutMS",
                            &opts->maxRPCTimeoutMS);
  conf->GetValueFatalIfFail("executorOpt.maxRetrySleepIntervalUS",
                            &opts->maxRetrySleepIntervalUS);
  conf->GetValueFatalIfFail("executorOpt.minRetryTimesForceTimeoutBackoff",
                            &opts->minRetryTimesForceTimeoutBackoff);
  conf->GetValueFatalIfFail("executorOpt.maxRetryTimesBeforeConsiderSuspend",
                            &opts->maxRetryTimesBeforeConsiderSuspend);
  conf->GetValueFatalIfFail("executorOpt.batchInodeAttrLimit",
                            &opts->batchInodeAttrLimit);
  conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                            &opts->enableRenameParallel);
}

void InitBlockDeviceOption(Configuration* conf,
                           BlockDeviceClientOptions* bdevOpt) {
  conf->GetValueFatalIfFail("bdev.confPath", &bdevOpt->configPath);
}

void InitS3Option(Configuration* conf, S3Option* s3Opt) {
  conf->GetValueFatalIfFail("s3.fakeS3", &FLAGS_useFakeS3);
  conf->GetValueFatalIfFail("data_stream.page.size",
                            &s3Opt->s3ClientAdaptorOpt.pageSize);
  conf->GetValueFatalIfFail("s3.prefetchBlocks",
                            &s3Opt->s3ClientAdaptorOpt.prefetchBlocks);
  conf->GetValueFatalIfFail("s3.prefetchExecQueueNum",
                            &s3Opt->s3ClientAdaptorOpt.prefetchExecQueueNum);
  conf->GetValueFatalIfFail("data_stream.background_flush.interval_ms",
                            &s3Opt->s3ClientAdaptorOpt.intervalMs);
  conf->GetValueFatalIfFail("data_stream.slice.stay_in_memory_max_second",
                            &s3Opt->s3ClientAdaptorOpt.flushIntervalSec);
  conf->GetValueFatalIfFail("s3.writeCacheMaxByte",
                            &s3Opt->s3ClientAdaptorOpt.writeCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheMaxByte",
                            &s3Opt->s3ClientAdaptorOpt.readCacheMaxByte);
  conf->GetValueFatalIfFail("s3.readCacheThreads",
                            &s3Opt->s3ClientAdaptorOpt.readCacheThreads);
  conf->GetValueFatalIfFail("s3.nearfullRatio",
                            &s3Opt->s3ClientAdaptorOpt.nearfullRatio);
  conf->GetValueFatalIfFail("s3.baseSleepUs",
                            &s3Opt->s3ClientAdaptorOpt.baseSleepUs);
  conf->GetValueFatalIfFail("s3.maxReadRetryIntervalMs",
                            &s3Opt->s3ClientAdaptorOpt.maxReadRetryIntervalMs);
  conf->GetValueFatalIfFail("s3.readRetryIntervalMs",
                            &s3Opt->s3ClientAdaptorOpt.readRetryIntervalMs);
  dingofs::aws::InitS3AdaptorOptionExceptS3InfoOption(conf,
                                                      &s3Opt->s3AdaptrOpt);
}

void InitVolumeOption(Configuration* conf, VolumeOption* volumeOpt) {
  conf->GetValueFatalIfFail("volume.bigFileSize", &volumeOpt->bigFileSize);
  conf->GetValueFatalIfFail("volume.volBlockSize", &volumeOpt->volBlockSize);
  conf->GetValueFatalIfFail("volume.fsBlockSize", &volumeOpt->fsBlockSize);
  conf->GetValueFatalIfFail("volume.allocator.type",
                            &volumeOpt->allocatorOption.type);

  conf->GetValueFatalIfFail(
      "volume.blockGroup.allocateOnce",
      &volumeOpt->allocatorOption.blockGroupOption.allocateOnce);

  if (volumeOpt->allocatorOption.type == "bitmap") {
    conf->GetValueFatalIfFail(
        "volume.bitmapAllocator.sizePerBit",
        &volumeOpt->allocatorOption.bitmapAllocatorOption.sizePerBit);
    conf->GetValueFatalIfFail(
        "volume.bitmapAllocator.smallAllocProportion",
        &volumeOpt->allocatorOption.bitmapAllocatorOption.smallAllocProportion);
  } else {
    CHECK(false) << "only support bitmap allocator";
  }
}

void InitExtentManagerOption(Configuration* conf,
                             ExtentManagerOption* extentManagerOpt) {
  conf->GetValueFatalIfFail("extentManager.preAllocSize",
                            &extentManagerOpt->preAllocSize);
}

void InitLeaseOpt(Configuration* conf, LeaseOpt* leaseOpt) {
  conf->GetValueFatalIfFail("mds.leaseTimesUs", &leaseOpt->leaseTimeUs);
  conf->GetValueFatalIfFail("mds.refreshTimesPerLease",
                            &leaseOpt->refreshTimesPerLease);
}

void InitRefreshDataOpt(Configuration* conf, RefreshDataOption* opt) {
  conf->GetValueFatalIfFail("fuseClient.maxDataSize", &opt->maxDataSize);
  conf->GetValueFatalIfFail("fuseClient.refreshDataIntervalSec",
                            &opt->refreshDataIntervalSec);
}

void InitKVClientManagerOpt(Configuration* conf, KVClientManagerOpt* config) {
  conf->GetValueFatalIfFail("fuseClient.supportKVcache", &FLAGS_supportKVcache);
  conf->GetValueFatalIfFail("fuseClient.setThreadPool",
                            &config->setThreadPooln);
  conf->GetValueFatalIfFail("fuseClient.getThreadPool",
                            &config->getThreadPooln);
}

void InitFileSystemOption(Configuration* c, FileSystemOption* option) {
  c->GetValueFatalIfFail("fs.cto", &option->cto);
  c->GetValueFatalIfFail("fs.cto", &FLAGS_enableCto);
  c->GetValueFatalIfFail("fs.nocto_suffix", &option->nocto_suffix);
  c->GetValueFatalIfFail("fs.disableXAttr", &option->disableXAttr);
  c->GetValueFatalIfFail("fs.maxNameLength", &option->maxNameLength);
  c->GetValueFatalIfFail("fs.accessLogging", &FLAGS_access_logging);
  {  // kernel cache option
    auto o = &option->kernelCacheOption;
    c->GetValueFatalIfFail("fs.kernelCache.attrTimeoutSec", &o->attrTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.dirAttrTimeoutSec",
                           &o->dirAttrTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.entryTimeoutSec",
                           &o->entryTimeoutSec);
    c->GetValueFatalIfFail("fs.kernelCache.dirEntryTimeoutSec",
                           &o->dirEntryTimeoutSec);
  }
  {  // lookup cache option
    auto o = &option->lookupCacheOption;
    c->GetValueFatalIfFail("fs.lookupCache.lruSize", &o->lruSize);
    c->GetValueFatalIfFail("fs.lookupCache.negativeTimeoutSec",
                           &o->negativeTimeoutSec);
    c->GetValueFatalIfFail("fs.lookupCache.minUses", &o->minUses);
  }
  {  // dir cache option
    auto o = &option->dirCacheOption;
    c->GetValueFatalIfFail("fs.dirCache.lruSize", &o->lruSize);
  }
  {  // attr watcher option
    auto o = &option->attrWatcherOption;
    c->GetValueFatalIfFail("fs.attrWatcher.lruSize", &o->lruSize);
  }
  {  // rpc option
    auto o = &option->rpcOption;
    c->GetValueFatalIfFail("fs.rpc.listDentryLimit", &o->listDentryLimit);
  }
  {  // defer sync option
    auto o = &option->deferSyncOption;
    c->GetValueFatalIfFail("fs.deferSync.delay", &o->delay);
    c->GetValueFatalIfFail("fs.deferSync.deferDirMtime", &o->deferDirMtime);
  }
}

void InitDataStreamOption(Configuration* c, DataStreamOption* option) {
  {  // background flush option
    auto* o = &option->background_flush_option;
    c->GetValueFatalIfFail(
        "data_stream.background_flush.trigger_force_memory_ratio",
        &o->trigger_force_memory_ratio);
  }
  {  // file option
    auto* o = &option->file_option;
    c->GetValueFatalIfFail("data_stream.file.flush_workers", &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.file.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // chunk option
    auto* o = &option->chunk_option;
    c->GetValueFatalIfFail("data_stream.chunk.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.chunk.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // slice option
    auto* o = &option->slice_option;
    c->GetValueFatalIfFail("data_stream.slice.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.slice.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // page option
    auto* o = &option->page_option;
    c->GetValueFatalIfFail("data_stream.page.size", &o->page_size);
    c->GetValueFatalIfFail("data_stream.page.total_size_mb", &o->total_size);
    c->GetValueFatalIfFail("data_stream.page.use_pool", &o->use_pool);

    if (o->page_size == 0) {
      CHECK(false) << "Page size must greater than 0.";
    }

    o->total_size = o->total_size * kMiB;
    if (o->total_size < 64 * kMiB) {
      CHECK(false) << "Page total size must greater than 64MiB.";
    }

    double trigger_force_flush_memory_ratio =
        option->background_flush_option.trigger_force_memory_ratio;
    if (o->total_size * (1.0 - trigger_force_flush_memory_ratio) < 32 * kMiB) {
      CHECK(false) << "Please gurantee the free memory size greater than 32MiB "
                      "before force flush.";
    }
  }
}

namespace {

void SplitDiskCacheOption(DiskCacheOption option,
                          std::vector<DiskCacheOption>* options) {
  std::vector<std::string> dirs = StrSplit(option.cache_dir, ";");
  for (size_t i = 0; i < dirs.size(); i++) {
    uint64_t cache_size = option.cache_size;
    std::vector<std::string> items = StrSplit(dirs[i], ":");
    if (items.size() > 2 ||
        (items.size() == 2 && !Str2Int(items[1], &cache_size))) {
      CHECK(false) << "Invalid cache dir: " << dirs[i];
    } else if (cache_size == 0) {
      CHECK(false) << "Cache size must greater than 0.";
    }

    DiskCacheOption o = option;
    o.index = i;
    o.cache_dir = items[0];
    o.cache_size = cache_size * kMiB;
    options->emplace_back(o);
  }
}

}  // namespace

void InitBlockCacheOption(Configuration* c, BlockCacheOption* option) {
  {  // block cache option
    c->GetValueFatalIfFail("block_cache.stage", &option->stage);
    c->GetValueFatalIfFail("block_cache.stage_bandwidth_throttle_enable",
                           &FLAGS_block_cache_stage_bandwidth_throttle_enable);
    c->GetValueFatalIfFail("block_cache.stage_bandwidth_throttle_mb",
                           &FLAGS_block_cache_stage_bandwidth_throttle_mb);
    c->GetValueFatalIfFail("block_cache.logging", &FLAGS_block_cache_logging);
    c->GetValueFatalIfFail("block_cache.upload_stage_workers",
                           &option->upload_stage_workers);
    c->GetValueFatalIfFail("block_cache.upload_stage_queue_size",
                           &option->upload_stage_queue_size);
    c->GetValueFatalIfFail("block_cache.cache_store", &option->cache_store);
    if (option->cache_store != "none" && option->cache_store != "disk") {
      CHECK(false) << "Only support disk or none cache store.";
    }
  }

  {  // disk cache option
    DiskCacheOption o;
    c->GetValueFatalIfFail("disk_cache.cache_dir", &o.cache_dir);
    c->GetValueFatalIfFail("disk_cache.cache_size_mb", &o.cache_size);
    c->GetValueFatalIfFail("disk_cache.free_space_ratio",
                           &FLAGS_disk_cache_free_space_ratio);
    c->GetValueFatalIfFail("disk_cache.cache_expire_second",
                           &FLAGS_disk_cache_expire_second);
    c->GetValueFatalIfFail(
        "disk_cache.cleanup_expire_interval_millsecond",
        &FLAGS_disk_cache_cleanup_expire_interval_millsecond);
    c->GetValueFatalIfFail("disk_cache.drop_page_cache",
                           &FLAGS_drop_page_cache);
    if (option->cache_store == "disk") {
      SplitDiskCacheOption(o, &option->disk_cache_options);
    }
  }

  {  // disk state option
    c->GetValueFatalIfFail("disk_state.tick_duration_second",
                           &FLAGS_disk_state_tick_duration_second);
    c->GetValueFatalIfFail("disk_state.normal2unstable_io_error_num",
                           &FLAGS_disk_state_normal2unstable_io_error_num);
    c->GetValueFatalIfFail("disk_state.unstable2normal_io_succ_num",
                           &FLAGS_disk_state_unstable2normal_io_succ_num);
    c->GetValueFatalIfFail("disk_state.unstable2down_second",
                           &FLAGS_disk_state_unstable2down_second);
    c->GetValueFatalIfFail("disk_state.disk_check_duration_millsecond",
                           &FLAGS_disk_check_duration_millsecond);
  }
}

void SetBrpcOpt(Configuration* conf) {
  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(conf, "defer_close_second", "rpc.defer.close.second",
             &brpc::FLAGS_defer_close_second);
  dummy.Load(conf, "health_check_interval", "rpc.healthCheckIntervalSec",
             &brpc::FLAGS_health_check_interval);
}

void InitFuseClientOption(Configuration* conf, FuseClientOption* clientOption) {
  InitMdsOption(conf, &clientOption->mdsOpt);
  InitMetaCacheOption(conf, &clientOption->metaCacheOpt);
  InitExcutorOption(conf, &clientOption->excutorOpt, false);
  InitExcutorOption(conf, &clientOption->excutorInternalOpt, true);
  InitBlockDeviceOption(conf, &clientOption->bdevOpt);
  InitS3Option(conf, &clientOption->s3Opt);
  InitExtentManagerOption(conf, &clientOption->extentManagerOpt);
  InitVolumeOption(conf, &clientOption->volumeOpt);
  InitLeaseOpt(conf, &clientOption->leaseOpt);
  InitRefreshDataOpt(conf, &clientOption->refreshDataOption);
  InitKVClientManagerOpt(conf, &clientOption->kvClientManagerOpt);
  InitFileSystemOption(conf, &clientOption->fileSystemOption);
  InitDataStreamOption(conf, &clientOption->data_stream_option);
  InitBlockCacheOption(conf, &clientOption->block_cache_option);

  conf->GetValueFatalIfFail("fuseClient.listDentryLimit",
                            &clientOption->listDentryLimit);
  conf->GetValueFatalIfFail("fuseClient.listDentryThreads",
                            &clientOption->listDentryThreads);
  conf->GetValueFatalIfFail("client.dummyServer.startPort",
                            &clientOption->dummyServerStartPort);
  conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                            &clientOption->enableMultiMountPointRename);
  conf->GetValueFatalIfFail("fuseClient.downloadMaxRetryTimes",
                            &clientOption->downloadMaxRetryTimes);
  conf->GetValueFatalIfFail("fuseClient.warmupThreadsNum",
                            &clientOption->warmupThreadsNum);
  LOG_IF(WARNING, conf->GetBoolValue("fuseClient.enableSplice",
                                     &clientOption->enableFuseSplice))
      << "Not found `fuseClient.enableSplice` in conf, use default value `"
      << std::boolalpha << clientOption->enableFuseSplice << '`';

  conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteBytes",
                            &FLAGS_fuseClientAvgWriteBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytes",
                            &FLAGS_fuseClientBurstWriteBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytesSecs",
                            &FLAGS_fuseClientBurstWriteBytesSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteIops",
                            &FLAGS_fuseClientAvgWriteIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIops",
                            &FLAGS_fuseClientBurstWriteIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIopsSecs",
                            &FLAGS_fuseClientBurstWriteIopsSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgReadBytes",
                            &FLAGS_fuseClientAvgReadBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytes",
                            &FLAGS_fuseClientBurstReadBytes);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytesSecs",
                            &FLAGS_fuseClientBurstReadBytesSecs);

  conf->GetValueFatalIfFail("fuseClient.throttle.avgReadIops",
                            &FLAGS_fuseClientAvgReadIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIops",
                            &FLAGS_fuseClientBurstReadIops);
  conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIopsSecs",
                            &FLAGS_fuseClientBurstReadIopsSecs);
  SetBrpcOpt(conf);
}

void SetFuseClientS3Option(FuseClientOption* clientOption,
                           const S3InfoOption& fsS3Opt) {
  clientOption->s3Opt.s3ClientAdaptorOpt.blockSize = fsS3Opt.blockSize;
  clientOption->s3Opt.s3ClientAdaptorOpt.chunkSize = fsS3Opt.chunkSize;
  clientOption->s3Opt.s3ClientAdaptorOpt.objectPrefix = fsS3Opt.objectPrefix;
  clientOption->s3Opt.s3AdaptrOpt.s3Address = fsS3Opt.s3Address;
  clientOption->s3Opt.s3AdaptrOpt.ak = fsS3Opt.ak;
  clientOption->s3Opt.s3AdaptrOpt.sk = fsS3Opt.sk;
  clientOption->s3Opt.s3AdaptrOpt.bucketName = fsS3Opt.bucketName;
}

void S3Info2FsS3Option(const pb::common::S3Info& s3, S3InfoOption* fsS3Opt) {
  fsS3Opt->ak = s3.ak();
  fsS3Opt->sk = s3.sk();
  fsS3Opt->s3Address = s3.endpoint();
  fsS3Opt->bucketName = s3.bucketname();
  fsS3Opt->blockSize = s3.blocksize();
  fsS3Opt->chunkSize = s3.chunksize();
  fsS3Opt->objectPrefix = s3.has_objectprefix() ? s3.objectprefix() : 0;
}

void RewriteCacheDir(BlockCacheOption* option, std::string uuid) {
  auto& disk_cache_options = option->disk_cache_options;
  for (auto& disk_cache_option : disk_cache_options) {
    std::string cache_dir = disk_cache_option.cache_dir;
    disk_cache_option.cache_dir = PathJoin({cache_dir, uuid});
  }
}

}  // namespace common
}  // namespace client
}  // namespace dingofs