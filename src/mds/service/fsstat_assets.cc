// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include "mds/service/fsstat_assets.h"

#include <cctype>
#include <string>

#include "brpc/http_status_code.h"
#include "common/logging.h"

namespace dingofs {
namespace mds {
namespace {

bool IsConsoleEntryRoute(const std::string& route) {
  if (route.empty() || route == "filesystems" || route == "mds" || route == "clients" || route == "cache-members" ||
      route == "server-details" || route == "version" || route == "locks" || route == "id-generators" ||
      route == "cache-summary" || route == "tools/parse-key") {
    return true;
  }
  constexpr std::string_view kFilesystemPrefix = "filesystems/";
  if (route.rfind(kFilesystemPrefix, 0) != 0) return false;
  const auto rest = route.substr(kFilesystemPrefix.size());
  const auto first_slash = rest.find('/');
  if (first_slash == std::string_view::npos) return !rest.empty();
  if (first_slash == 0) return false;
  const auto fs_id = rest.substr(0, first_slash);
  const auto diagnostic = rest.substr(first_slash + 1);
  if (diagnostic == "details" || diagnostic == "tree" || diagnostic == "quota" || diagnostic == "dir-stats" ||
      diagnostic == "mountpoints" || diagnostic == "file-sessions" || diagnostic == "deleted-files" ||
      diagnostic == "deleted-slices" || diagnostic == "slice-references" || diagnostic == "oplog") {
    return true;
  }
  for (const auto prefix : {std::string_view("deleted-files/"), std::string_view("inodes/")}) {
    if (diagnostic.rfind(prefix, 0) == 0 && diagnostic.size() > prefix.size()) return true;
  }
  constexpr std::string_view kFilesPrefix = "files/";
  if (diagnostic.rfind(kFilesPrefix, 0) != 0) return false;
  const auto file_rest = diagnostic.substr(kFilesPrefix.size());
  const auto file_slash = file_rest.find('/');
  if (file_slash == std::string_view::npos || file_slash == 0 || file_slash + 1 == file_rest.size()) return false;
  const auto file_id = file_rest.substr(0, file_slash);
  const auto file_diagnostic = file_rest.substr(file_slash + 1);
  return !fs_id.empty() && !file_id.empty() && (file_diagnostic == "chunks" || file_diagnostic == "shard");
}

std::string UrlEncodeQueryValue(const std::string& value) {
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string encoded;
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded.push_back(static_cast<char>(c));
    } else {
      encoded.push_back('%');
      encoded.push_back(kHex[c >> 4]);
      encoded.push_back(kHex[c & 0x0F]);
    }
  }
  return encoded;
}

void SetConsoleSecurityHeaders(brpc::Controller* controller) {
  auto& response = controller->http_response();
  response.SetHeader("Content-Security-Policy",
                     "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
                     "img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; "
                     "object-src 'none'; frame-ancestors 'none'");
  response.SetHeader("X-Content-Type-Options", "nosniff");
  response.SetHeader("Referrer-Policy", "no-referrer");
  response.SetHeader("X-Frame-Options", "DENY");
}

}  // namespace

bool ServeFsStatConsole(brpc::Controller* controller, const std::string& route, butil::IOBufBuilder& os) {
  const bool is_asset = route.rfind("assets/", 0) == 0;
  if (!is_asset && !IsConsoleEntryRoute(route)) {
    return false;
  }

  SetConsoleSecurityHeaders(controller);

  if (route.empty()) {
    const std::string* key = controller->http_request().uri().GetQuery("key");
    if (key != nullptr) {
      controller->http_response().set_status_code(brpc::HTTP_STATUS_FOUND);
      controller->http_response().SetHeader("Location", "/FsStatService/legacy?key=" + UrlEncodeQueryValue(*key));
      controller->http_response().set_content_type("text/plain");
      controller->http_response().SetHeader("Cache-Control", "no-store");
      return true;
    }
  }

  const std::string asset_path = is_asset ? route : "index.html";
  const FsStatAsset* asset = FindFsStatAsset(asset_path);
  if (asset == nullptr) {
    controller->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
    controller->http_response().set_content_type("text/plain");
    controller->http_response().SetHeader("Cache-Control", "no-store");
    os << "Console asset not found.";
    return true;
  }

  auto& response = controller->http_response();
  response.set_content_type(std::string(asset->content_type));
  response.SetHeader("ETag", std::string(asset->etag));
  if (asset_path == "index.html") {
    response.SetHeader("Cache-Control", "no-cache");
  } else {
    response.SetHeader("Cache-Control", "public, max-age=31536000, immutable");
  }

  const std::string* if_none_match = controller->http_request().GetHeader("If-None-Match");
  if (if_none_match != nullptr && *if_none_match == asset->etag) {
    response.set_status_code(brpc::HTTP_STATUS_NOT_MODIFIED);
    return true;
  }

  os.write(reinterpret_cast<const char*>(asset->data), static_cast<std::streamsize>(asset->size));
  return true;
}

}  // namespace mds
}  // namespace dingofs
