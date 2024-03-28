//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  auto minTime = maxTimestamp;
  auto findLessKFrame = false;
  for (const auto &it : frameRecords) {
    if (!frameEvictable[it.first])
      continue;
    if (it.second->size() < k_) {
      if (!findLessKFrame || it.second->front() < minTime) {
        findLessKFrame = true;
        minTime = it.second->front();
        *frame_id = it.first;
      }
    } else if (!findLessKFrame) {
      if (it.second->front() < minTime) {
        minTime = it.second->front();
        *frame_id = it.first;
      }
    }
  }
  if (minTime == maxTimestamp) {
    latch_.unlock();
    return false;
  }
  latch_.unlock();
  Remove(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(unsigned(frame_id) <= replacer_size_, "frame_id should > replacer_size_");
  if (frameRecords.size() == replacer_size_ && frameRecords.count(frame_id) == 0) {
    auto tmp = std::make_unique<frame_id_t>();
    Evict(tmp.get());
  }
  if (frameRecords.count(frame_id) == 0) {
    frameRecords[frame_id] = std::make_shared<std::queue<size_t>>();
    frameEvictable[frame_id] = true;
    evictableNum++;
  }
  frameRecords[frame_id]->push(++current_timestamp_);
  if (frameRecords[frame_id]->size() > k_)
    frameRecords[frame_id]->pop();
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(frameEvictable.count(frame_id) == 1, "frameEvictable should have frame_id");
  if (set_evictable && !frameEvictable[frame_id])
    evictableNum++;
  else if (!set_evictable && frameEvictable[frame_id])
    evictableNum--;
  frameEvictable[frame_id] = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (frameEvictable.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  BUSTUB_ASSERT(frameEvictable[frame_id] == true, "frame_id should be able to evitable");
  frameRecords.erase(frame_id);
  frameEvictable.erase(frame_id);
  evictableNum--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return evictableNum; }

}  // namespace bustub
