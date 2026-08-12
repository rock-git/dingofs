// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "mds/service/fsstat_diagnostics_api.h"

#include <json/value.h>
#include <json/writer.h>

#include <cstdint>
#include <ctime>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "brpc/http_status_code.h"
#include "fmt/format.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/type.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/fs_utils.h"
#include "mds/server.h"
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

bool ParseIds(const std::vector<std::string>& params, size_t fs_index, uint32_t& fs_id, uint64_t* ino,
              size_t ino_index) {
  if (params.size() <= fs_index) return false;
  fs_id = Helper::StringToInt32(params[fs_index]);
  if (fs_id == 0) return false;
  if (ino != nullptr) {
    if (params.size() <= ino_index) return false;
    *ino = Helper::StringToUint64(params[ino_index]);
    if (*ino == 0) return false;
  }
  return true;
}

Json::Value RenderQuotaValue(const QuotaEntry& quota) {
  Json::Value value(Json::objectValue);
  value["maxBytes"] = std::to_string(quota.max_bytes());
  value["usedBytes"] = std::to_string(quota.used_bytes());
  value["maxInodes"] = std::to_string(quota.max_inodes());
  value["usedInodes"] = std::to_string(quota.used_inodes());
  return value;
}

Json::Value RenderMountPoints(const FsInfoEntry& fs_info) {
  Json::Value items(Json::arrayValue);
  for (const auto& mountpoint : fs_info.mount_points()) {
    Json::Value item(Json::objectValue);
    item["clientId"] = mountpoint.client_id();
    item["hostname"] = mountpoint.hostname();
    item["ip"] = mountpoint.ip();
    item["port"] = mountpoint.port();
    item["path"] = mountpoint.path();
    item["cto"] = mountpoint.cto();
    items.append(std::move(item));
  }
  return items;
}

Json::Value RenderPartition(const Json::Value& partition) {
  Json::Value result(Json::objectValue);
  for (const auto& name : {"fs_id", "ino", "base_version", "delta_version", "delta_dentry_ops_count"}) {
    if (!partition[name].isNull()) result[name] = partition[name].asString();
  }
  Json::Value shards(Json::arrayValue);
  for (const auto& shard : partition["shards"]) {
    Json::Value item(Json::objectValue);
    for (const auto& name : {"start", "end"}) item[name] = shard[name].asString();
    for (const auto& name : {"id", "size", "version"}) {
      if (!shard[name].isNull()) item[name] = shard[name].asString();
    }
    shards.append(std::move(item));
  }
  result["shards"] = std::move(shards);
  return result;
}

Json::Value RenderChunks(const std::vector<ChunkEntry>& chunks) {
  Json::Value items(Json::arrayValue);
  for (const auto& chunk : chunks) {
    Json::Value item(Json::objectValue);
    item["index"] = chunk.index();
    item["chunkSizeBytes"] = std::to_string(chunk.chunk_size());
    item["blockSizeBytes"] = std::to_string(chunk.block_size());
    item["version"] = std::to_string(chunk.version());
    item["lastCompactionAt"] = Iso8601Ms(chunk.last_compaction_time_ms());
    Json::Value slices(Json::arrayValue);
    for (const auto& slice : chunk.slices()) {
      Json::Value slice_value(Json::objectValue);
      slice_value["id"] = std::to_string(slice.id());
      slice_value["pos"] = slice.pos();
      slice_value["size"] = slice.size();
      slice_value["off"] = slice.off();
      slice_value["len"] = slice.len();
      slices.append(std::move(slice_value));
    }
    item["slices"] = std::move(slices);
    items.append(std::move(item));
  }
  return items;
}

bool IsPrefix(const std::vector<std::string>& params) {
  return params.size() >= 5 && params[0] == "api" && params[1] == "v1" && params[2] == "filesystems";
}

}  // namespace

bool HandleFsStatDiagnosticsApi(brpc::Controller* controller, const std::vector<std::string>& params,
                                butil::IOBufBuilder& os) {
  if (!IsPrefix(params)) return false;
  if (controller->http_request().method() != brpc::HTTP_METHOD_GET) {
    WriteError(controller, brpc::HTTP_STATUS_METHOD_NOT_ALLOWED, "method_not_allowed", "Only GET is supported.", os);
    return true;
  }

  uint32_t fs_id = 0;
  uint64_t ino = 0;
  if (!ParseIds(params, 3, fs_id, nullptr, 0)) {
    WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_filesystem_id", "The file system ID is invalid.",
               os);
    return true;
  }

  auto file_system_set = Server::GetInstance().GetFileSystemSet();
  auto file_system = file_system_set->GetFileSystem(fs_id);
  if (file_system == nullptr) {
    WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "filesystem_not_found",
               fmt::format("File system {} was not found.", fs_id), os);
    return true;
  }

  PrepareResponse(controller);
  const std::string& resource = params[4];
  if (params.size() == 5 && resource == "quota") {
    Trace trace;
    QuotaEntry fs_quota;
    auto status = file_system->GetQuotaManager().GetFsQuota(trace, false, fs_quota);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "quota_unavailable", status.error_str(), os);
      return true;
    }
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["filesystem"] = RenderQuotaValue(fs_quota);
    std::map<Ino, QuotaEntry> dirs;
    status = file_system->GetQuotaManager().LoadDirQuotas(trace, dirs);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "directory_quota_unavailable", status.error_str(),
                 os);
      return true;
    }
    Json::Value directory_items(Json::arrayValue);
    for (const auto& [dir_ino, quota] : dirs) {
      Json::Value item = RenderQuotaValue(quota);
      item["ino"] = std::to_string(dir_ino);
      directory_items.append(std::move(item));
    }
    root["directories"] = std::move(directory_items);
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  if (params.size() == 5 && resource == "dir-stats") {
    std::map<Ino, DirStatEntry> stats;
    auto status = file_system->GetDirStatManager().GetAllDirStats(stats);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "dir_stats_unavailable", status.error_str(), os);
      return true;
    }
    Json::Value items(Json::arrayValue);
    for (const auto& [dir_ino, stat] : stats) {
      if (stat.length() == 0 && stat.inodes() == 0 && stat.dirs() == 0) continue;
      Json::Value item(Json::objectValue);
      item["ino"] = std::to_string(dir_ino);
      item["lengthBytes"] = std::to_string(stat.length());
      item["inodes"] = std::to_string(stat.inodes());
      item["directories"] = std::to_string(stat.dirs());
      items.append(std::move(item));
    }
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["items"] = std::move(items);
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  if (params.size() == 5 && resource == "mountpoints") {
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["items"] = RenderMountPoints(file_system->GetFsInfo());
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  if (params.size() == 5 && resource == "file-sessions") {
    std::vector<FileSessionEntry> sessions;
    auto status = file_system_set->GetFileSessions(fs_id, sessions);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "file_sessions_unavailable", status.error_str(),
                 os);
      return true;
    }
    Json::Value items(Json::arrayValue);
    for (const auto& session : sessions) {
      Json::Value item(Json::objectValue);
      item["ino"] = std::to_string(session.ino());
      item["sessionId"] = session.session_id();
      item["clientId"] = session.client_id();
      item["createdAt"] = Iso8601Ms(session.create_time_s() * 1000);
      item["expiresAt"] = Iso8601Ms(session.expire_time_s() * 1000);
      items.append(std::move(item));
    }
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["items"] = std::move(items);
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  if (params.size() == 7 && resource == "files" && params[5] != "" && params[6] == "chunks") {
    if (!ParseIds(params, 3, fs_id, &ino, 5)) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_inode", "The inode is invalid.", os);
      return true;
    }
    FsUtils fs_utils(Server::GetInstance().GetOperationProcessor());
    std::vector<ChunkEntry> chunks;
    auto status = fs_utils.GetChunks(fs_id, ino, chunks);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "chunks_unavailable", status.error_str(), os);
      return true;
    }
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["ino"] = std::to_string(ino);
    root["items"] = RenderChunks(chunks);
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  if (params.size() == 7 && resource == "files" && params[5] != "" && params[6] == "shard") {
    if (!ParseIds(params, 3, fs_id, &ino, 5)) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_inode", "The inode is invalid.", os);
      return true;
    }
    Json::Value partition;
    auto status = file_system->DescribePartitionShard(ino, partition);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "shard_unavailable", status.error_str(), os);
      return true;
    }
    Json::Value root(Json::objectValue);
    root["filesystemId"] = std::to_string(fs_id);
    root["ino"] = std::to_string(ino);
    root["partition"] = RenderPartition(partition);
    root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
    WriteJson(root, os);
    return true;
  }

  WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "not_found", "Diagnostic resource not found.", os);
  return true;
}

}  // namespace mds
}  // namespace dingofs
