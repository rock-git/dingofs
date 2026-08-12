// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_MDS_SERVICE_FSSTAT_ASSETS_H_
#define DINGOFS_MDS_SERVICE_FSSTAT_ASSETS_H_

#include <cstddef>
#include <string_view>

#include "brpc/controller.h"
#include "butil/iobuf.h"

namespace dingofs {
namespace mds {

struct FsStatAsset {
  std::string_view path;
  const unsigned char* data;
  std::size_t size;
  std::string_view content_type;
  std::string_view etag;
};

const FsStatAsset* FindFsStatAsset(std::string_view path);

// Serves the embedded React console and its hashed assets. Returns true for
// console routes, including a 404 response for an unknown console asset.
bool ServeFsStatConsole(brpc::Controller* controller, const std::string& route, butil::IOBufBuilder& os);

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_FSSTAT_ASSETS_H_
