// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "mds/service/fsstat_remaining_api.h"

#include <json/reader.h>
#include <json/value.h>
#include <json/writer.h>

#include <cstdint>
#include <ctime>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "brpc/http_status_code.h"
#include "common/version.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "mds/common/codec.h"
#include "mds/common/context.h"
#include "mds/common/distribution_lock.h"
#include "mds/common/helper.h"
#include "mds/common/type.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/fs_utils.h"
#include "mds/server.h"
#include "mds/storage/dingodb_storage.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace {

std::string RequestId(const brpc::Controller* controller) {
  if (!controller->request_id().empty()) return controller->request_id();
  if (controller->log_id() != 0) return std::to_string(controller->log_id());
  return "unknown";
}

void PrepareResponse(brpc::Controller* controller) {
  auto& response = controller->http_response();
  response.set_content_type("application/json; charset=utf-8");
  response.SetHeader("Cache-Control", "no-store");
  response.SetHeader("X-Content-Type-Options", "nosniff");
  response.SetHeader("Referrer-Policy", "no-referrer");
  response.SetHeader("X-Request-Id", RequestId(controller));
}

void WriteJson(const Json::Value& value, butil::IOBufBuilder& os) {
  Json::StreamWriterBuilder writer;
  writer["indentation"] = "";
  writer["emitUTF8"] = true;
  os << Json::writeString(writer, value);
}

void WriteError(brpc::Controller* controller, int status_code, const std::string& code, const std::string& message,
                butil::IOBufBuilder& os) {
  PrepareResponse(controller);
  controller->http_response().set_status_code(status_code);
  Json::Value root(Json::objectValue);
  root["error"]["code"] = code;
  root["error"]["message"] = message;
  root["error"]["requestId"] = RequestId(controller);
  WriteJson(root, os);
}

std::string Iso8601Ms(uint64_t timestamp_ms) {
  if (timestamp_ms == 0) return "";
  const std::time_t seconds = static_cast<std::time_t>(timestamp_ms / 1000);
  std::tm tm_value{};
  if (gmtime_r(&seconds, &tm_value) == nullptr) return "";
  return fmt::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", tm_value.tm_year + 1900, tm_value.tm_mon + 1,
                     tm_value.tm_mday, tm_value.tm_hour, tm_value.tm_min, tm_value.tm_sec, timestamp_ms % 1000);
}

std::string NsToIso8601(uint64_t timestamp_ns) { return Iso8601Ms(timestamp_ns / 1000000); }

bool ParseUint(const std::string& value, uint64_t max_value, uint64_t& output) {
  if (value.empty()) return false;
  try {
    size_t consumed = 0;
    const auto parsed = std::stoull(value, &consumed);
    if (consumed != value.size() || parsed > max_value) return false;
    output = parsed;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool ParseFilesystemId(const std::vector<std::string>& params, size_t index, uint32_t& fs_id) {
  uint64_t value = 0;
  if (params.size() <= index || !ParseUint(params[index], std::numeric_limits<uint32_t>::max(), value) || value == 0) {
    return false;
  }
  fs_id = static_cast<uint32_t>(value);
  return true;
}

bool ParseInode(const std::vector<std::string>& params, size_t index, uint64_t& ino) {
  return params.size() > index && ParseUint(params[index], std::numeric_limits<uint64_t>::max(), ino);
}

Json::Value RenderMountPoint(const pb::mds::MountPoint& mountpoint) {
  Json::Value value(Json::objectValue);
  value["clientId"] = mountpoint.client_id();
  value["hostname"] = mountpoint.hostname();
  value["ip"] = mountpoint.ip();
  value["port"] = mountpoint.port();
  value["path"] = mountpoint.path();
  value["cto"] = mountpoint.cto();
  return value;
}

Json::Value RenderStorage(const pb::mds::FsExtra& extra) {
  Json::Value value(Json::objectValue);
  const auto& s3 = extra.s3_info();
  const auto& rados = extra.rados_info();
  if (!s3.endpoint().empty()) {
    value["type"] = "s3";
    value["endpoint"] = s3.endpoint();
    value["bucket"] = s3.bucketname();
  } else if (!rados.mon_host().empty()) {
    value["type"] = "rados";
    value["endpoint"] = rados.mon_host();
    value["pool"] = rados.pool_name();
    value["user"] = rados.user_name();
    value["cluster"] = rados.cluster_name();
  } else if (!extra.file_info().path().empty()) {
    value["type"] = "local-file";
    value["path"] = extra.file_info().path();
  } else {
    value["type"] = "unknown";
  }
  return value;
}

Json::Value RenderPartitionPolicy(const pb::mds::PartitionPolicy& policy) {
  Json::Value value(Json::objectValue);
  value["type"] = pb::mds::PartitionType_Name(policy.type());
  value["epoch"] = std::to_string(policy.epoch());
  if (policy.type() == pb::mds::MONOLITHIC_PARTITION) {
    value["mdsId"] = std::to_string(policy.mono().mds_id());
  } else if (policy.type() == pb::mds::PARENT_ID_HASH_PARTITION) {
    value["bucketNum"] = policy.parent_hash().bucket_num();
    value["expectedMdsNum"] = policy.parent_hash().expect_mds_num();
    Json::Value distributions(Json::arrayValue);
    for (const auto& [mds_id, bucket_set] : policy.parent_hash().distributions()) {
      Json::Value distribution(Json::objectValue);
      distribution["mdsId"] = std::to_string(mds_id);
      Json::Value buckets(Json::arrayValue);
      for (const auto bucket_id : bucket_set.bucket_ids()) buckets.append(bucket_id);
      distribution["bucketIds"] = std::move(buckets);
      distributions.append(std::move(distribution));
    }
    value["distributions"] = std::move(distributions);
  }
  return value;
}

Json::Value RenderFilesystemDetails(const FsInfoEntry& fs_info) {
  Json::Value value(Json::objectValue);
  value["id"] = std::to_string(fs_info.fs_id());
  value["name"] = fs_info.fs_name();
  value["uuid"] = fs_info.uuid();
  value["type"] = pb::mds::FsType_Name(fs_info.fs_type());
  value["lifecycleState"] = pb::mds::FsStatus_Name(fs_info.status());
  value["owner"] = fs_info.owner();
  value["rootIno"] = std::to_string(fs_info.root_ino());
  value["capacityBytes"] = std::to_string(fs_info.capacity());
  value["blockSizeBytes"] = std::to_string(fs_info.block_size());
  value["chunkSizeBytes"] = std::to_string(fs_info.chunk_size());
  value["version"] = std::to_string(fs_info.version());
  value["createdAt"] = Iso8601Ms(fs_info.create_time_s() * 1000);
  value["updatedAt"] = NsToIso8601(fs_info.last_update_time_ns());
  value["deletedAt"] = Iso8601Ms(fs_info.delete_time_s() * 1000);
  value["recycleTimeHours"] = fs_info.recycle_time_hour();
  value["trashDays"] = fs_info.trash_days();
  value["enableDirStats"] = fs_info.enable_dir_stats();
  value["enableUidGidMap"] = fs_info.enable_uid_gid_map();
  value["immediateTrashQuota"] = fs_info.immediate_trash_quota();
  value["partitionPolicy"] = RenderPartitionPolicy(fs_info.partition_policy());
  value["storage"] = RenderStorage(fs_info.extra());
  Json::Value mountpoints(Json::arrayValue);
  for (const auto& mountpoint : fs_info.mount_points()) mountpoints.append(RenderMountPoint(mountpoint));
  value["mountPoints"] = std::move(mountpoints);
  return value;
}

Json::Value RenderInode(const AttrEntry& attr) {
  Json::Value value(Json::objectValue);
  value["filesystemId"] = std::to_string(attr.fs_id());
  value["ino"] = std::to_string(attr.ino());
  value["type"] = pb::mds::FileType_Name(attr.type());
  value["lengthBytes"] = std::to_string(attr.length());
  value["ctime"] = NsToIso8601(attr.ctime());
  value["mtime"] = NsToIso8601(attr.mtime());
  value["atime"] = NsToIso8601(attr.atime());
  value["uid"] = attr.uid();
  value["gid"] = attr.gid();
  value["mode"] = attr.mode();
  value["nlink"] = attr.nlink();
  value["rdev"] = std::to_string(attr.rdev());
  value["dtime"] = Iso8601Ms(static_cast<uint64_t>(attr.dtime()) * 1000);
  value["openCount"] = attr.openmpcount();
  value["version"] = std::to_string(attr.version());
  value["symlink"] = attr.symlink();
  value["sharedSlice"] = attr.shared_slice();
  Json::Value parents(Json::arrayValue);
  for (const auto parent : attr.parents()) parents.append(std::to_string(parent));
  value["parents"] = std::move(parents);
  Json::Value shard_boundaries(Json::arrayValue);
  for (const auto& boundary : attr.shard_boundaries()) shard_boundaries.append(boundary);
  value["shardBoundaries"] = std::move(shard_boundaries);
  Json::Value xattr_names(Json::arrayValue);
  for (const auto& [name, unused] : attr.xattrs()) {
    (void)unused;
    xattr_names.append(name);
  }
  value["xattrNames"] = std::move(xattr_names);
  return value;
}

Json::Value RenderDeletedFile(const AttrEntry& attr) {
  Json::Value value = RenderInode(attr);
  value["id"] = std::to_string(attr.ino());
  return value;
}

Json::Value RenderSlice(const SliceEntry& slice) {
  Json::Value value(Json::objectValue);
  value["id"] = std::to_string(slice.id());
  value["pos"] = slice.pos();
  value["size"] = slice.size();
  value["off"] = slice.off();
  value["len"] = slice.len();
  return value;
}

Json::Value RenderDeletedSlices(const std::vector<TrashSliceList>& lists) {
  Json::Value items(Json::arrayValue);
  for (const auto& list : lists) {
    for (const auto& deleted : list.slices()) {
      Json::Value value(Json::objectValue);
      value["filesystemId"] = std::to_string(deleted.fs_id());
      value["ino"] = std::to_string(deleted.ino());
      value["chunkIndex"] = std::to_string(deleted.chunk_index());
      value["blockSizeBytes"] = std::to_string(deleted.block_size());
      value["chunkSizeBytes"] = std::to_string(deleted.chunk_size());
      value["deletedAt"] = Iso8601Ms(list.time_ms());
      value["slice"] = RenderSlice(deleted.slice());
      items.append(std::move(value));
    }
  }
  return items;
}

Json::Value RenderSliceReferences(const std::vector<SliceRefEntry>& references) {
  Json::Value items(Json::arrayValue);
  for (const auto& reference : references) {
    Json::Value value(Json::objectValue);
    value["id"] = std::to_string(reference.id());
    value["sizeBytes"] = reference.size();
    value["refCount"] = reference.ref_count();
    Json::Value inodes(Json::arrayValue);
    for (const auto ino : reference.inos()) inodes.append(std::to_string(ino));
    value["inodes"] = std::move(inodes);
    items.append(std::move(value));
  }
  return items;
}

Json::Value RenderMdsIds(const google::protobuf::RepeatedField<uint64_t>& ids) {
  Json::Value values(Json::arrayValue);
  for (const auto id : ids) values.append(std::to_string(id));
  return values;
}

Json::Value RenderOpLog(const FsOpLog& log) {
  Json::Value value(Json::objectValue);
  value["filesystemId"] = std::to_string(log.fs_id());
  value["filesystemName"] = log.fs_name();
  value["type"] = FsOpLog::Type_Name(log.type());
  value["epoch"] = std::to_string(log.epoch());
  value["comment"] = log.comment();
  Json::Value parameter(Json::objectValue);
  if (log.has_join_fs()) {
    parameter["mdsIds"] = RenderMdsIds(log.join_fs().mds_ids());
  } else if (log.has_quit_fs()) {
    parameter["mdsIds"] = RenderMdsIds(log.quit_fs().mds_ids());
  } else if (log.has_quit_and_join_fs()) {
    parameter["quitMdsIds"] = RenderMdsIds(log.quit_and_join_fs().quit_mds_ids());
    parameter["joinMdsIds"] = RenderMdsIds(log.quit_and_join_fs().join_mds_ids());
  } else if (log.has_update_state_fs()) {
    parameter["oldState"] = pb::mds::FsStatus_Name(log.update_state_fs().old_status());
    parameter["newState"] = pb::mds::FsStatus_Name(log.update_state_fs().new_status());
  }
  value["parameter"] = std::move(parameter);
  value["time"] = Iso8601Ms(log.time_ms());
  return value;
}

Json::Value RenderTreeItems(const std::string& serialized) {
  Json::Value parsed;
  Json::Reader reader;
  if (!reader.parse(serialized, parsed) || !parsed.isArray()) return Json::Value(Json::arrayValue);
  Json::Value items(Json::arrayValue);
  for (const auto& entry : parsed) {
    Json::Value item(Json::objectValue);
    for (const auto& name : {"ino", "name", "type", "node", "description", "no_shard"}) {
      if (!entry[name].isNull()) item[name] = entry[name];
    }
    if (!item["ino"].isNull() && !item["ino"].isString()) item["ino"] = std::to_string(entry["ino"].asUInt64());
    if (!item["node"].isNull() && !item["node"].isString()) item["node"] = std::to_string(entry["node"].asUInt64());
    items.append(std::move(item));
  }
  return items;
}

Json::Value RenderServerDetails() {
  Json::Value raw(Json::objectValue);
  Server::GetInstance().DescribeByJson(raw);
  Json::Value value(Json::objectValue);
  value["selfMds"] = raw["b-self_mds_meta"];
  value["mdsMetaMap"] = raw["c-mds_meta_map"];
  value["filesystemCaches"] = raw["d-file_system_set"];
  value["crontab"] = raw["e-crontab"];
  value["allocator"] = raw["a-tcmalloc"];
  return value;
}

Json::Value RenderCacheSummary() {
  Json::Value raw(Json::arrayValue);
  Server::GetInstance().GetFileSystemSet()->Summary(raw);
  Json::Value items(Json::arrayValue);
  for (const auto& filesystem : raw) {
    const std::string fs_id = std::to_string(filesystem["fsid"].asUInt());
    const std::string fs_name = filesystem["fs_name"].asString();
    for (const auto& cache : filesystem["caches"]) {
      Json::Value item(Json::objectValue);
      item["filesystemId"] = fs_id;
      item["filesystemName"] = fs_name;
      item["name"] = cache["name"].asString();
      for (const auto& field : {"count", "total_count", "clean_count", "bytes", "miss_count", "hit_count"}) {
        if (!cache[field].isNull()) item[field] = std::to_string(cache[field].asUInt64());
      }
      if (!cache["miss_count"].isNull() && !cache["hit_count"].isNull()) {
        const auto misses = cache["miss_count"].asUInt64();
        const auto hits = cache["hit_count"].asUInt64();
        item["hitRatio"] = hits + misses == 0 ? 0.0 : hits * 100.0 / (hits + misses);
      }
      items.append(std::move(item));
    }
  }
  return items;
}

bool IsRemainingRoute(const std::vector<std::string>& params) {
  if (params.size() < 3 || params[0] != "api" || params[1] != "v1") return false;
  if (params.size() == 3) {
    return params[2] == "server" || params[2] == "version" || params[2] == "locks" || params[2] == "id-generators" ||
           params[2] == "cache-summary";
  }
  if (params.size() == 4 && params[2] == "tools" && params[3] == "parse-key") return true;
  if (params.size() < 5 || params[2] != "filesystems") return false;
  if (params.size() == 5) {
    return params[4] == "details" || params[4] == "tree" || params[4] == "deleted-files" ||
           params[4] == "deleted-slices" || params[4] == "slice-references" || params[4] == "oplog";
  }
  return params.size() == 6 && (params[4] == "deleted-files" || params[4] == "inodes");
}

bool GetFilesystem(uint32_t fs_id, FileSystemSPtr& filesystem, brpc::Controller* controller, butil::IOBufBuilder& os) {
  filesystem = Server::GetInstance().GetFileSystemSet()->GetFileSystem(fs_id);
  if (filesystem != nullptr) return true;
  WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "filesystem_not_found",
             fmt::format("File system {} was not found.", fs_id), os);
  return false;
}

}  // namespace

bool HandleFsStatRemainingApi(brpc::Controller* controller, const std::vector<std::string>& params,
                              butil::IOBufBuilder& os) {
  if (!IsRemainingRoute(params)) return false;
  if (controller->http_request().method() != brpc::HTTP_METHOD_GET) {
    WriteError(controller, brpc::HTTP_STATUS_METHOD_NOT_ALLOWED, "method_not_allowed", "Only GET is supported.", os);
    return true;
  }

  PrepareResponse(controller);
  Json::Value root(Json::objectValue);
  root["generatedAt"] = Iso8601Ms(utils::TimestampMs());

  if (params.size() == 3 && params[2] == "server") {
    root["server"] = RenderServerDetails();
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 3 && params[2] == "version") {
    Json::Value build(Json::arrayValue);
    for (const auto& [name, value] : DingoVersion()) {
      Json::Value item(Json::objectValue);
      item["name"] = name;
      item["value"] = value;
      build.append(std::move(item));
    }
    Json::Value sdk(Json::arrayValue);
    for (const auto& [name, value] : DingodbStorage::GetSdkVersion()) {
      Json::Value item(Json::objectValue);
      item["name"] = name;
      item["value"] = value;
      sdk.append(std::move(item));
    }
    root["build"] = std::move(build);
    root["sdk"] = std::move(sdk);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 3 && params[2] == "locks") {
    std::vector<StoreDistributionLock::LockEntry> locks;
    const auto status = StoreDistributionLock::GetAllLockInfo(Server::GetInstance().GetOperationProcessor(), locks);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "locks_unavailable", status.error_str(), os);
      return true;
    }
    Json::Value items(Json::arrayValue);
    for (const auto& lock : locks) {
      Json::Value item(Json::objectValue);
      item["name"] = lock.name;
      item["owner"] = std::to_string(lock.owner);
      item["epoch"] = std::to_string(lock.epoch);
      item["expiresAt"] = Iso8601Ms(lock.expire_time_ms);
      items.append(std::move(item));
    }
    root["items"] = std::move(items);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 3 && params[2] == "id-generators") {
    Json::Value items(Json::arrayValue);
    Server::GetInstance().GetFileSystemSet()->DescribeIdGenerators(items);
    root["items"] = std::move(items);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 3 && params[2] == "cache-summary") {
    root["items"] = RenderCacheSummary();
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 4 && params[2] == "tools" && params[3] == "parse-key") {
    const auto* key = controller->http_request().uri().GetQuery("key");
    root["key"] = key != nullptr ? *key : "";
    root["result"] = key != nullptr ? MetaCodec::ParseKeyFromHex(*key) : "";
    WriteJson(root, os);
    return true;
  }

  uint32_t fs_id = 0;
  if (!ParseFilesystemId(params, 3, fs_id)) {
    WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_filesystem_id", "The file system ID is invalid.",
               os);
    return true;
  }
  FileSystemSPtr filesystem;
  if (!GetFilesystem(fs_id, filesystem, controller, os)) return true;

  if (params.size() == 5 && params[4] == "details") {
    root["filesystem"] = RenderFilesystemDetails(filesystem->GetFsInfo());
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 5 && params[4] == "tree") {
    const auto* parent_query = controller->http_request().uri().GetQuery("parentIno");
    uint64_t parent_ino = 0;
    if (parent_query != nullptr && !ParseUint(*parent_query, std::numeric_limits<uint64_t>::max(), parent_ino)) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_parent_inode", "The parent inode is invalid.", os);
      return true;
    }
    std::string serialized;
    FsUtils fs_utils(Server::GetInstance().GetOperationProcessor(), filesystem->GetFsInfo());
    const auto status = fs_utils.GenDirJsonString(parent_ino, serialized);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "directory_unavailable", status.error_str(), os);
      return true;
    }
    root["parentIno"] = std::to_string(parent_ino);
    root["items"] = RenderTreeItems(serialized);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 5 && params[4] == "deleted-files") {
    std::vector<AttrEntry> deleted_files;
    const auto status = Server::GetInstance().GetFileSystemSet()->GetDelFiles(fs_id, deleted_files);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "deleted_files_unavailable", status.error_str(),
                 os);
      return true;
    }
    root["items"] = Json::Value(Json::arrayValue);
    for (const auto& attr : deleted_files) root["items"].append(RenderDeletedFile(attr));
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 5 && params[4] == "deleted-slices") {
    std::vector<TrashSliceList> deleted_slices;
    const auto status = Server::GetInstance().GetFileSystemSet()->GetDelSlices(fs_id, deleted_slices);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "deleted_slices_unavailable", status.error_str(),
                 os);
      return true;
    }
    root["items"] = RenderDeletedSlices(deleted_slices);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 5 && params[4] == "slice-references") {
    std::vector<SliceRefEntry> references;
    const auto status = Server::GetInstance().GetFileSystemSet()->GetSliceRefs(references);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "slice_references_unavailable", status.error_str(),
                 os);
      return true;
    }
    root["items"] = RenderSliceReferences(references);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 5 && params[4] == "oplog") {
    std::vector<FsOpLog> logs;
    const auto status = Server::GetInstance().GetFileSystemSet()->GetFsOpLogs(fs_id, logs);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "oplog_unavailable", status.error_str(), os);
      return true;
    }
    root["items"] = Json::Value(Json::arrayValue);
    for (const auto& log : logs) root["items"].append(RenderOpLog(log));
    WriteJson(root, os);
    return true;
  }

  uint64_t ino = 0;
  if (params.size() == 6 && params[4] == "deleted-files") {
    if (!ParseInode(params, 5, ino)) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_inode", "The inode is invalid.", os);
      return true;
    }
    AttrEntry attr;
    const auto status = filesystem->GetDeletedFileForManagement(ino, attr);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "inode_not_found", status.error_str(), os);
      return true;
    }
    root["inode"] = RenderInode(attr);
    WriteJson(root, os);
    return true;
  }
  if (params.size() == 6 && params[4] == "inodes") {
    if (!ParseInode(params, 5, ino)) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_inode", "The inode is invalid.", os);
      return true;
    }
    InodeSPtr inode;
    Context context("fsstatservice", "management-console");
    const auto status = filesystem->GetInodeForManagement(context, ino, "ManagementConsole", inode);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "inode_not_found", status.error_str(), os);
      return true;
    }
    root["inode"] = RenderInode(inode->ToAttr());
    WriteJson(root, os);
    return true;
  }
  WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "not_found", "Management API route not found.", os);
  return true;
}

}  // namespace mds
}  // namespace dingofs
