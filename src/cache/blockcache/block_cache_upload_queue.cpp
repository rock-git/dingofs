/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_upload_queue.h"

#include <glog/logging.h>

namespace dingofs {
namespace cache {
namespace blockcache {

void PendingQueue::Push(const StageBlock& stage_block) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto from = stage_block.ctx.from;
  auto iter = queues_.find(from);
  if (iter == queues_.end()) {
    iter = queues_.emplace(from, Segments<StageBlock>(kSegmentSize)).first;
  }

  auto& queue = iter->second;
  queue.Push(stage_block);
  count_[from]++;
}

std::vector<StageBlock> PendingQueue::Pop(bool peek) {
  static std::vector<BlockFrom> pop_prority{
      BlockFrom::kCtoFlush,
      BlockFrom::kNoctoFlush,
      BlockFrom::kReload,
      BlockFrom::kUnknown,
  };

  std::unique_lock<std::mutex> lk(mutex_);
  for (const auto& from : pop_prority) {
    auto iter = queues_.find(from);
    if (iter != queues_.end() && iter->second.Size() != 0) {
      auto stage_blocks = iter->second.Pop(peek);
      if (!peek) {
        CHECK(count_[from] >= stage_blocks.size());
        count_[from] -= stage_blocks.size();
      }
      return stage_blocks;
    }
  }
  return std::vector<StageBlock>();
}

size_t PendingQueue::Size() {
  std::unique_lock<std::mutex> lk(mutex_);
  size_t size = 0;
  for (auto& item : queues_) {
    size += item.second.Size();
  }
  return size;
}

void PendingQueue::Stat(struct StatBlocks* stat) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto num_from_cto = count_[BlockFrom::kCtoFlush];
  auto num_from_nocto = count_[BlockFrom::kNoctoFlush];
  auto num_from_reload = count_[BlockFrom::kReload];
  auto num_total = num_from_cto + num_from_nocto + num_from_reload;
  *stat = StatBlocks(num_total, num_from_cto, num_from_nocto, num_from_reload);
}

UploadingQueue::UploadingQueue(size_t capacity)
    : running_(false), capacity_(capacity) {}

void UploadingQueue::Start() {
  running_.exchange(true, std::memory_order_acq_rel);
}

void UploadingQueue::Stop() {
  if (running_.exchange(false, std::memory_order_acq_rel)) {
    not_empty_.notify_all();
  }
}

void UploadingQueue::Push(const StageBlock& stage_block) {
  std::unique_lock<std::mutex> lk(mutex_);
  while (queue_.size() == capacity_) {  // full
    not_full_.wait(lk);
  }
  queue_.push(stage_block);
  count_[stage_block.ctx.from]++;
  not_empty_.notify_one();
}

StageBlock UploadingQueue::Pop() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (queue_.empty() && running_.load(std::memory_order_acquire)) {
    not_empty_.wait(lk);
  }

  if (queue_.empty()) {  // For queue stopped
    return StageBlock();
  }

  auto stage_block = queue_.top();
  queue_.pop();
  CHECK(count_[stage_block.ctx.from] > 0);
  count_[stage_block.ctx.from]--;
  not_full_.notify_one();
  return stage_block;
}

size_t UploadingQueue::Size() {
  std::unique_lock<std::mutex> lk(mutex_);
  return queue_.size();
}

void UploadingQueue::Stat(struct StatBlocks* stat) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto num_from_cto = count_[BlockFrom::kCtoFlush];
  auto num_from_nocto = count_[BlockFrom::kNoctoFlush];
  auto num_from_reload = count_[BlockFrom::kReload];
  auto num_total = num_from_cto + num_from_nocto + num_from_reload;
  *stat = StatBlocks(num_total, num_from_cto, num_from_nocto, num_from_reload);
}

size_t UploadingQueue::Capacity() const { return capacity_; }

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
