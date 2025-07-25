/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_CONFIG_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_CONFIG_H_

#include <cstdint>

namespace dingofs {
namespace mds {
namespace cachegroup {

struct CacheGroupOption {
  uint64_t heartbeat_interval_s;
  uint64_t heartbeat_miss_timeout_s;
  uint64_t heartbeat_offline_timeout_s;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_CONFIG_H_
