// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_UTILS_EXECUTOR_H_
#define DINGOFS_UTILS_EXECUTOR_H_

#include <functional>
#include <string>

namespace dingofs {

class Executor {
 public:
  virtual ~Executor() = default;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual bool Execute(std::function<void()> func) = 0;

  virtual bool Schedule(std::function<void()> func, int delay_ms) = 0;

  virtual int ThreadNum() const = 0;

  virtual int TaskNum() const = 0;

  virtual std::string Name() const = 0;
};

}  // namespace dingofs

#endif  // DINGOFS_UTILS_EXECUTOR_H_