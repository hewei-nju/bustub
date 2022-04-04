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
    this->mtx_.lock();
    if (this->lst_.empty()) {
        frame_id = nullptr;
        this->mtx_.unlock();
        return false;
    }
    *frame_id = this->lst_.back();
    this->lru_.erase(*frame_id);
    this->lst_.pop_back();
    this->mtx_.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    this->mtx_.lock();
    if (this->lru_.find(frame_id) != this->lru_.end()) {
        this->lst_.erase(this->lru_[frame_id]);
        this->lru_.erase(frame_id);
    }
    this->mtx_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    this->mtx_.lock();
    if (this->lru_.find(frame_id) == this->lru_.end()) {
        if (this->num_pages_ == this->lst_.size()) {
            this->lru_.erase(this->lst_.back());
            this->lst_.pop_back();
        }
        this->lst_.push_front(frame_id);
        this->lru_.insert({frame_id, this->lst_.begin()});
    }
    this->mtx_.unlock();
}

size_t LRUReplacer::Size() { return this->lst_.size(); }

}  // namespace bustub
