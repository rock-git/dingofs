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

#ifndef DINGOFS_DATA_ACCESS_ACCESSER_COMMON_H_
#define DINGOFS_DATA_ACCESS_ACCESSER_COMMON_H_

#include <functional>
#include <memory>
#include <string>

#include "client/common/status.h"

namespace dingofs {
namespace dataaccess {

using ::dingofs::client::Status;

struct GetObjectAsyncContext;
struct PutObjectAsyncContext;

using PutObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<PutObjectAsyncContext>&)>;

struct PutObjectAsyncContext {
  std::string key;
  const char* buffer;
  size_t buffer_size;
  uint64_t start_time;
  int ret_code;

  PutObjectAsyncCallBack cb;
};

using GetObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<GetObjectAsyncContext>&)>;

struct GetObjectAsyncContext {
  std::string key;
  char* buf;
  off_t offset;
  size_t len;
  int ret_code;
  uint32_t retry;
  size_t actual_len;

  GetObjectAsyncCallBack cb;
};

}  // namespace dataaccess
}  // namespace dingofs

#endif  // DINGOFS_DATA_ACCESS_ACCESSER_COMMON_H_
