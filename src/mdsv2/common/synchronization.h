// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_MDSV2_COMMON_SYNCHRONIZATION_H_
#define DINGOFS_MDSV2_COMMON_SYNCHRONIZATION_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <queue>

#include "bthread/bthread.h"
#include "bthread/butex.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "glog/logging.h"
#include "mdsv2/common/logging.h"

namespace dingofs {

namespace mdsv2 {

class BthreadCond {
 public:
  BthreadCond(int count = 0);
  ~BthreadCond();
  int Count() const;

  void Increase();
  void DecreaseSignal();
  void DecreaseBroadcast();
  int Wait(int cond = 0);
  int IncreaseWait(int cond = 0);
  int TimedWait(int64_t timeout_us, int cond = 0);
  int IncreaseTimedWait(int64_t timeout_us, int cond = 0);

 private:
  int count_;
  bthread_cond_t cond_;
  bthread_mutex_t mutex_;
};

using BthreadCondPtr = std::shared_ptr<BthreadCond>;

class BthreadSemaphore {
 public:
  BthreadSemaphore(int initial_value = 0) {
    butex_ = bthread::butex_create_checked<int>();
    *butex_ = initial_value;
  }
  ~BthreadSemaphore() { bthread::butex_destroy(butex_); }

  int GetValue() const { return ((butil::atomic<int>*)butex_)->load(butil::memory_order_acquire); }

  void Release(int value) {
    CHECK(value > 0) << "Invalid value:" << value;

    ((butil::atomic<int>*)butex_)->fetch_add(value, butil::memory_order_release);
    bthread::butex_wake(butex_, false);
  }

  void Acquire() {
    for (;;) {
      int curr_value = ((butil::atomic<int>*)butex_)->load(butil::memory_order_acquire);
      if (curr_value <= 0) {
        if (bthread::butex_wait(butex_, curr_value, nullptr) < 0 && errno != EWOULDBLOCK && errno != EINTR) {
          DINGO_LOG(ERROR) << "butex_wait failed, errno: " << errno;
        }
      } else {
        if (((butil::atomic<int>*)butex_)
                ->compare_exchange_strong(curr_value, curr_value - 1, butil::memory_order_release)) {
          break;
        }
      }
    }
  }

 private:
  int* butex_;
};

// wrapper bthread functions for c++ style
class Bthread {
 public:
  Bthread() = default;
  explicit Bthread(const bthread_attr_t* attr);
  explicit Bthread(const std::function<void()>& call);
  explicit Bthread(const bthread_attr_t* attr, const std::function<void()>& call);

  void Run(const std::function<void()>& call);

  void RunUrgent(const std::function<void()>& call);

  void Join() const;

  bthread_t Id() const;

 private:
  bthread_t tid_;
  const bthread_attr_t* attr_ = nullptr;
};

// RAII
class ScopeGuard {
 public:
  explicit ScopeGuard(std::function<void()> exit_func);
  ~ScopeGuard();

  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;

  void Release();

 private:
  std::function<void()> exit_func_;
  bool is_release_ = false;
};

#define SCOPEGUARD_LINENAME_CAT(name, line) name##line
#define SCOPEGUARD_LINENAME(name, line) SCOPEGUARD_LINENAME_CAT(name, line)
#define ON_SCOPE_EXIT(callback) ScopeGuard SCOPEGUARD_LINENAME(scope_guard, __LINE__)(callback)
#define DEFER(expr) ON_SCOPE_EXIT([&]() { expr; })

class RWLock {
 private:
  bthread_mutex_t mutex_;       // mutex to protect the following fields
  bthread_cond_t cond_;         // condition variable
  int active_readers_ = 0;      // number of active readers
  int waiting_writers_ = 0;     // number of waiting writers
  bool active_writer_ = false;  // is there an active writer

  bool CanRead() const;

  bool CanWrite() const;

 public:
  RWLock();
  ~RWLock();

  void LockRead();

  void UnlockRead();

  void LockWrite();

  void UnlockWrite();
};

class RWLockReadGuard {
 public:
  explicit RWLockReadGuard(RWLock* rw_lock);
  ~RWLockReadGuard();

  RWLockReadGuard(const RWLockReadGuard&) = delete;
  RWLockReadGuard& operator=(const RWLockReadGuard&) = delete;

  void Release();

 private:
  RWLock* rw_lock_;
  bool is_release_ = false;
};

class RWLockWriteGuard {
 public:
  explicit RWLockWriteGuard(RWLock* rw_lock);
  ~RWLockWriteGuard();

  RWLockWriteGuard(const RWLockWriteGuard&) = delete;
  RWLockWriteGuard& operator=(const RWLockWriteGuard&) = delete;

  void Release();

 private:
  RWLock* rw_lock_;
  bool is_release_ = false;
};

template <typename T>
class ResourcePool {
 public:
  explicit ResourcePool(const std::string& name) {
    bthread_mutex_init(&mutex_, nullptr);
    bthread_cond_init(&cond_, nullptr);
    pool_size_ = new bvar::Adder<int64_t>(name + "_POOL_SIZE");
  }

  ~ResourcePool() {
    bthread_mutex_destroy(&mutex_);
    bthread_cond_destroy(&cond_);
    delete pool_size_;
  }

  // Insert a resource into the pool
  void Put(const T& item) {
    BAIDU_SCOPED_LOCK(mutex_);
    pool_.push(item);
    (*pool_size_) << 1;
    bthread_cond_broadcast(&cond_);
  }

  // Get a resource from the pool
  T Get() {
    BAIDU_SCOPED_LOCK(mutex_);
    while (pool_.empty()) {
      bthread_cond_wait(&cond_, &mutex_);
    }
    T item = pool_.front();
    pool_.pop();
    (*pool_size_) << -1;
    return item;
  }

 private:
  bthread_mutex_t mutex_;  // mutex to protect the following fields
  bthread_cond_t cond_;    // condition variable
  std::queue<T> pool_;
  bvar::Adder<int64_t>* pool_size_;
};

class AtomicGuard {
 public:
  AtomicGuard(std::atomic<bool>& flag) : m_flag_(flag) { m_flag_.store(true); }
  ~AtomicGuard() {
    if (!released_) {
      m_flag_.store(false);
    }
  }

  void Release() { released_ = true; }

 private:
  bool released_ = false;
  std::atomic<bool>& m_flag_;
};

class BvarLatencyGuard {
 public:
  explicit BvarLatencyGuard(bvar::LatencyRecorder* latency_recoder);
  ~BvarLatencyGuard();

  BvarLatencyGuard(const BvarLatencyGuard&) = delete;
  BvarLatencyGuard& operator=(const BvarLatencyGuard&) = delete;

  void Release();

 private:
  bvar::LatencyRecorder* latency_recorder_;
  int64_t start_time_us_;
  bool is_release_ = false;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_SYNCHRONIZATION_H_
