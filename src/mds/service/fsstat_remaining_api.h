// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_MDS_SERVICE_FSSTAT_REMAINING_API_H_
#define DINGOFS_MDS_SERVICE_FSSTAT_REMAINING_API_H_

#include <string>
#include <vector>

#include "brpc/controller.h"
#include "butil/iobuf.h"

namespace dingofs {
namespace mds {

// Handles the read-only resources migrated from the remaining legacy FsStat
// pages. Returns false for routes owned by another handler.
bool HandleFsStatRemainingApi(brpc::Controller* controller, const std::vector<std::string>& params,
                              butil::IOBufBuilder& os);

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_FSSTAT_REMAINING_API_H_
