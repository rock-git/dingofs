// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/mdsv2/service/mds_meta.h"

#include "fmt/core.h"

namespace dingofs {

namespace mdsv2 {

std::string MDSMeta::ToString() const {
  return fmt::format("MDSMeta[id={}, host={}, port={}, state={}, register_time_ms={}, last_online_time_ms={}]", id_,
                     host_, port_, static_cast<int>(state_), register_time_ms_, last_online_time_ms_);
}

}  // namespace mdsv2

}  // namespace dingofs