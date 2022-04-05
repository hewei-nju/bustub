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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->lst_.empty()) {
    // frame_id = nullptr;  // this will make memory leak
    return false;
  }
  *frame_id = this->lst_.back();
  this->lru_.erase(*frame_id);
  this->lst_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->lru_.find(frame_id) != this->lru_.end()) {
    this->lst_.erase(this->lru_[frame_id]);
    this->lru_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->lru_.find(frame_id) == this->lru_.end()) {
    if (this->num_pages_ == this->lst_.size()) {
      this->lru_.erase(this->lst_.back());
      this->lst_.pop_back();
    }
    this->lst_.push_front(frame_id);
    this->lru_.insert({frame_id, this->lst_.begin()});
  }
}

size_t LRUReplacer::Size() { return this->lst_.size(); }

}  // namespace bustub
