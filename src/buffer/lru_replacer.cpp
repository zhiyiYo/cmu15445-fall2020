//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
using namespace std;

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<shared_mutex> lock(mutex_);

  if (Size() == 0) {
    return false;
  }

  *frame_id = list_.back();
  list_.pop_back();
  map_.erase(*frame_id);

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard<shared_mutex> lock(mutex_);

  // frame 需要在缓冲池中
  if (!map_.count(frame_id)) {
    return;
  }

  auto it = map_[frame_id];
  map_.erase(frame_id);
  list_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<shared_mutex> lock(mutex_);

  // 不缓冲池满了不能插入新的 page，能重复插入 page
  if (Size() == num_pages_ || map_.count(frame_id)) {
    return;
  }

  list_.push_front(frame_id);
  map_[frame_id] = list_.begin();
}

size_t LRUReplacer::Size() {
  return list_.size();
}

}  // namespace bustub
