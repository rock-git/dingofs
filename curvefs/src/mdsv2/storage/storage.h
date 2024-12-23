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

#ifndef DINGOFS_MDV2_STORAGE_H_
#define DINGOFS_MDV2_STORAGE_H_

#include <memory>
#include <string>

#include "curvefs/src/mdsv2/common/status.h"

namespace dingofs {

namespace mdsv2 {

class KVStorage {
 public:
  virtual ~KVStorage() = default;

  virtual Status Put(const std::string& key, const std::string& value) = 0;

  virtual Status Get(const std::string& key, std::string& value) = 0;

  virtual Status Delete(const std::string& key) = 0;
};
using KVStoragePtr = std::shared_ptr<KVStorage>;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_STORAGE_H_