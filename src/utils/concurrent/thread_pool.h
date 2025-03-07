/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: 18-9-26
 * Author: wudemiao
 */

#ifndef SRC_COMMON_CONCURRENT_THREAD_POOL_H_
#define SRC_COMMON_CONCURRENT_THREAD_POOL_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>   //NOLINT
#include <thread>  //NOLINT
#include <vector>

#include "utils/uncopyable.h"

namespace dingofs {
namespace utils {

class ThreadPool : public Uncopyable {
 public:
  ThreadPool();
  ~ThreadPool();

  int Init(int numThreads, std::function<void()> func);
  void Start();
  void Stop();
  int NumOfThreads();

 private:
  std::vector<std::unique_ptr<std::thread>> threads_;
  int numThreads_;
  std::function<void()> threadFunc_;
  std::atomic<bool> starting_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_CONCURRENT_THREAD_POOL_H_
