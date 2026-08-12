// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "mds/service/fsstat_api.h"

#include <json/value.h>
#include <json/writer.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <ctime>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "brpc/http_status_code.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "mds/common/context.h"
#include "mds/common/type.h"
#include "mds/filesystem/filesystem.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);
DECLARE_uint32(mds_heartbeat_client_offline_period_ms);
DECLARE_uint32(cache_member_heartbeat_offline_timeout_s);
DECLARE_uint32(cache_member_heartbeat_miss_timeout_s);
namespace {

constexpr size_t kDefaultLimit = 1000;
constexpr size_t kMaxLimit = 5000;

std::string Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

std::string Iso8601Ms(uint64_t timestamp_ms) {
  if (timestamp_ms == 0) return "";
  const std::time_t seconds = static_cast<std::time_t>(timestamp_ms / 1000);
  std::tm tm_value{};
  if (gmtime_r(&seconds, &tm_value) == nullptr) return "";
  return fmt::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", tm_value.tm_year + 1900, tm_value.tm_mon + 1,
                     tm_value.tm_mday, tm_value.tm_hour, tm_value.tm_min, tm_value.tm_sec, timestamp_ms % 1000);
}

std::string RequestId(const brpc::Controller* controller) {
  if (!controller->request_id().empty()) return controller->request_id();
  if (controller->log_id() != 0) return std::to_string(controller->log_id());
  return "unknown";
}

void PrepareJsonResponse(brpc::Controller* controller) {
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
  PrepareJsonResponse(controller);
  controller->http_response().set_status_code(status_code);
  Json::Value root(Json::objectValue);
  Json::Value error(Json::objectValue);
  error["code"] = code;
  error["message"] = message;
  error["requestId"] = RequestId(controller);
  root["error"] = error;
  WriteJson(root, os);
}

bool ParseLimitAndCursor(brpc::Controller* controller, size_t& limit, std::string& cursor, butil::IOBufBuilder& os) {
  limit = kDefaultLimit;
  const std::string* limit_query = controller->http_request().uri().GetQuery("limit");
  if (limit_query != nullptr && !limit_query->empty()) {
    try {
      const auto parsed = std::stoull(*limit_query);
      if (parsed == 0 || parsed > kMaxLimit) throw std::out_of_range("limit");
      limit = static_cast<size_t>(parsed);
    } catch (const std::exception&) {
      WriteError(controller, brpc::HTTP_STATUS_BAD_REQUEST, "invalid_limit",
                 fmt::format("limit must be between 1 and {}.", kMaxLimit), os);
      return false;
    }
  }
  const std::string* cursor_query = controller->http_request().uri().GetQuery("cursor");
  cursor = cursor_query != nullptr ? *cursor_query : "";
  return true;
}

template <typename T, typename KeyFn, typename RenderFn>
Json::Value BuildCollection(const std::vector<T>& values, const std::string& cursor, size_t limit, KeyFn key_fn,
                            RenderFn render_fn, Json::Value summary) {
  size_t start = 0;
  while (start < values.size() && !cursor.empty() && key_fn(values[start]) <= cursor) ++start;
  const size_t end = std::min(values.size(), start + limit);

  Json::Value root(Json::objectValue);
  root["summary"] = std::move(summary);
  Json::Value items(Json::arrayValue);
  for (size_t i = start; i < end; ++i) items.append(render_fn(values[i]));
  root["items"] = std::move(items);
  root["generatedAt"] = Iso8601Ms(utils::TimestampMs());
  root["truncated"] = end < values.size();
  if (end < values.size()) {
    root["nextCursor"] = key_fn(values[end - 1]);
  } else {
    root["nextCursor"] = Json::nullValue;
  }
  return root;
}

Json::Value Health(const std::string& state, const std::string& reason) {
  Json::Value value(Json::objectValue);
  value["state"] = state;
  value["reason"] = reason;
  return value;
}

void Increment(Json::Value& summary, const std::string& key) {
  summary["total"] = summary.get("total", 0).asUInt64() + 1;
  summary[key] = summary.get(key, 0).asUInt64() + 1;
}

Json::Value RenderStorage(const pb::mds::FsExtra& extra) {
  Json::Value storage(Json::objectValue);
  const auto& s3 = extra.s3_info();
  const auto& rados = extra.rados_info();
  if (!s3.endpoint().empty()) {
    storage["type"] = "s3";
    storage["endpoint"] = s3.endpoint();
    storage["bucket"] = s3.bucketname();
  } else if (!rados.mon_host().empty()) {
    storage["type"] = "rados";
    storage["endpoint"] = rados.mon_host();
    storage["pool"] = rados.pool_name();
    storage["cluster"] = rados.cluster_name();
  } else if (!extra.file_info().path().empty()) {
    storage["type"] = "local-file";
  } else {
    storage["type"] = "unknown";
  }
  return storage;
}

Json::Value RenderFilesystem(const pb::mds::FsInfo& fs_info) {
  Json::Value value(Json::objectValue);
  const auto lifecycle = Lowercase(pb::mds::FsStatus_Name(fs_info.status()));
  value["id"] = std::to_string(fs_info.fs_id());
  value["name"] = fs_info.fs_name();
  value["lifecycleState"] = lifecycle;
  value["type"] = Lowercase(pb::mds::FsType_Name(fs_info.fs_type()));
  value["partitionType"] = Lowercase(pb::mds::PartitionType_Name(fs_info.partition_policy().type()));
  value["owner"] = fs_info.owner();
  value["capacityBytes"] = std::to_string(fs_info.capacity());
  value["blockSizeBytes"] = std::to_string(fs_info.block_size());
  value["chunkSizeBytes"] = std::to_string(fs_info.chunk_size());
  value["mountPointCount"] = fs_info.mount_points_size();
  value["uuid"] = fs_info.uuid();
  value["version"] = std::to_string(fs_info.version());
  value["updatedAt"] = Iso8601Ms(fs_info.last_update_time_ns() / 1000000);
  value["createdAt"] = Iso8601Ms(fs_info.create_time_s() * 1000);
  value["health"] = Health(lifecycle, "filesystem_lifecycle");
  value["storage"] = RenderStorage(fs_info.extra());
  return value;
}

Json::Value RenderMds(const MdsEntry& mds, uint64_t now_ms) {
  const bool online = mds.last_online_time_ms() != 0 &&
                      mds.last_online_time_ms() + FLAGS_mds_heartbeat_mds_offline_period_time_ms >= now_ms;
  Json::Value value(Json::objectValue);
  value["id"] = std::to_string(mds.id());
  value["host"] = mds.location().host();
  value["port"] = mds.location().port();
  value["state"] = Lowercase(MdsEntry::State_Name(mds.state()));
  value["health"] = Health(online ? "online" : "offline", online ? "heartbeat_fresh" : "heartbeat_timeout");
  value["createdAt"] = Iso8601Ms(mds.create_time_ms());
  value["lastOnlineAt"] = Iso8601Ms(mds.last_online_time_ms());
  return value;
}

Json::Value RenderClient(const ClientEntry& client, uint64_t now_ms) {
  const bool online = client.last_online_time_ms() != 0 &&
                      client.last_online_time_ms() + FLAGS_mds_heartbeat_client_offline_period_ms >= now_ms;
  Json::Value value(Json::objectValue);
  value["id"] = client.id();
  value["host"] = client.ip();
  value["hostname"] = client.hostname();
  value["port"] = client.port();
  value["mountpoint"] = client.mountpoint();
  value["filesystem"] = client.fs_name();
  value["createdAt"] = Iso8601Ms(client.create_time_ms());
  value["lastOnlineAt"] = Iso8601Ms(client.last_online_time_ms());
  value["health"] = Health(online ? "online" : "offline", online ? "heartbeat_fresh" : "heartbeat_timeout");
  return value;
}

Json::Value RenderCacheMember(const CacheMemberEntry& member, uint64_t now_ms) {
  std::string state;
  std::string reason;
  if (member.last_online_time_ms() == 0) {
    state = "unknown";
    reason = "no_heartbeat";
  } else if (member.last_online_time_ms() + FLAGS_cache_member_heartbeat_offline_timeout_s * 1000 < now_ms) {
    state = "offline";
    reason = "heartbeat_timeout";
  } else if (member.last_online_time_ms() + FLAGS_cache_member_heartbeat_miss_timeout_s * 1000 < now_ms) {
    state = "unstable";
    reason = "heartbeat_missed";
  } else {
    state = "online";
    reason = "heartbeat_fresh";
  }
  Json::Value value(Json::objectValue);
  value["id"] = member.member_id();
  value["host"] = fmt::format("{}:{}", member.ip(), member.port());
  value["group"] = member.group_name();
  value["weight"] = member.weight();
  value["locked"] = member.locked();
  value["state"] = state;
  value["lastOnlineAt"] = Iso8601Ms(member.last_online_time_ms());
  value["health"] = Health(state, reason);
  return value;
}

bool IsRoute(const std::vector<std::string>& params, const char* resource) {
  return params.size() == 3 && params[0] == "api" && params[1] == "v1" && params[2] == resource;
}

}  // namespace

bool HandleFsStatApi(ManagementDataSource& data_source, brpc::Controller* controller,
                     const std::vector<std::string>& params, butil::IOBufBuilder& os) {
  if (params.size() < 2 || params[0] != "api" || params[1] != "v1") return false;
  PrepareJsonResponse(controller);
  if (controller->http_request().method() != brpc::HTTP_METHOD_GET) {
    WriteError(controller, brpc::HTTP_STATUS_METHOD_NOT_ALLOWED, "method_not_allowed", "Only GET is supported.", os);
    return true;
  }

  size_t limit = kDefaultLimit;
  std::string cursor;
  if (!ParseLimitAndCursor(controller, limit, cursor, os)) return true;

  if (IsRoute(params, "overview")) {
    Json::Value root(Json::objectValue);
    ManagementOverview overview;
    auto status = data_source.GetOverview(overview);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "overview_unavailable", status.error_str(), os);
      return true;
    }
    root["clusterId"] = std::to_string(overview.cluster_id);
    root["servingMdsId"] = std::to_string(overview.serving_mds_id);
    root["storageEngine"] = overview.storage_engine;
    root["apiVersion"] = overview.api_version;
    root["build"]["version"] = overview.build_version;
    root["build"]["commit"] = overview.build_commit;
    root["build"]["commitTime"] = overview.build_commit_time;
    WriteJson(root, os);
    return true;
  }

  Context ctx;
  const uint64_t now_ms = utils::TimestampMs();
  if (IsRoute(params, "filesystems")) {
    std::vector<pb::mds::FsInfo> values;
    auto status = data_source.GetFileSystems(ctx, values);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "filesystem_list_unavailable", status.error_str(),
                 os);
      return true;
    }
    std::sort(values.begin(), values.end(), [](const auto& lhs, const auto& rhs) { return lhs.fs_id() < rhs.fs_id(); });
    Json::Value summary(Json::objectValue);
    summary["total"] = 0;
    for (const auto& value : values) Increment(summary, Lowercase(pb::mds::FsStatus_Name(value.status())));
    WriteJson(BuildCollection(
                  values, cursor, limit, [](const auto& value) { return std::to_string(value.fs_id()); },
                  [](const auto& value) { return RenderFilesystem(value); }, std::move(summary)),
              os);
    return true;
  }

  if (IsRoute(params, "mds-nodes")) {
    std::vector<MdsEntry> values;
    auto status = data_source.GetMdsNodes(ctx, values);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "mds_list_unavailable", status.error_str(), os);
      return true;
    }
    std::sort(values.begin(), values.end(), [](const auto& lhs, const auto& rhs) { return lhs.id() < rhs.id(); });
    Json::Value summary(Json::objectValue);
    summary["total"] = 0;
    for (const auto& value : values)
      Increment(summary, value.last_online_time_ms() != 0 &&
                                 value.last_online_time_ms() + FLAGS_mds_heartbeat_mds_offline_period_time_ms >= now_ms
                             ? "online"
                             : "offline");
    WriteJson(BuildCollection(
                  values, cursor, limit, [](const auto& value) { return std::to_string(value.id()); },
                  [now_ms](const auto& value) { return RenderMds(value, now_ms); }, std::move(summary)),
              os);
    return true;
  }

  if (IsRoute(params, "clients")) {
    std::vector<ClientEntry> values;
    auto status = data_source.GetClients(values);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "client_list_unavailable", status.error_str(), os);
      return true;
    }
    std::sort(values.begin(), values.end(), [](const auto& lhs, const auto& rhs) { return lhs.id() < rhs.id(); });
    Json::Value summary(Json::objectValue);
    summary["total"] = 0;
    for (const auto& value : values)
      Increment(summary, value.last_online_time_ms() != 0 &&
                                 value.last_online_time_ms() + FLAGS_mds_heartbeat_client_offline_period_ms >= now_ms
                             ? "online"
                             : "offline");
    WriteJson(BuildCollection(
                  values, cursor, limit, [](const auto& value) { return value.id(); },
                  [now_ms](const auto& value) { return RenderClient(value, now_ms); }, std::move(summary)),
              os);
    return true;
  }

  if (IsRoute(params, "cache-members")) {
    std::vector<CacheMemberEntry> values;
    auto status = data_source.GetCacheMembers(values);
    if (!status.ok()) {
      WriteError(controller, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE, "cache_member_list_unavailable", status.error_str(),
                 os);
      return true;
    }
    std::sort(values.begin(), values.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.member_id() < rhs.member_id(); });
    Json::Value summary(Json::objectValue);
    summary["total"] = 0;
    for (const auto& value : values)
      Increment(summary, Lowercase(RenderCacheMember(value, now_ms)["state"].asString()));
    WriteJson(BuildCollection(
                  values, cursor, limit, [](const auto& value) { return value.member_id(); },
                  [now_ms](const auto& value) { return RenderCacheMember(value, now_ms); }, std::move(summary)),
              os);
    return true;
  }

  WriteError(controller, brpc::HTTP_STATUS_NOT_FOUND, "not_found", "Management API route not found.", os);
  return true;
}

}  // namespace mds
}  // namespace dingofs
