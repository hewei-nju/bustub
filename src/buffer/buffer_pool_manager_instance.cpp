//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(this->latch_);

  assert(page_id != INVALID_PAGE_ID);
  if (this->page_table_.find(page_id) == this->page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = this->page_table_[page_id];
  if (this->pages_[frame_id].IsDirty()) {
    this->pages_[frame_id].is_dirty_ = false;
    this->disk_manager_->WritePage(page_id, static_cast<const char *>(this->pages_[frame_id].data_));
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (const auto &entry : this->page_table_) {
    this->FlushPgImp(entry.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->free_list_.empty() && this->replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  frame_id_t frame_id = 0;
  *page_id = this->AllocatePage();
  if (!this->free_list_.empty()) {
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else {
    this->replacer_->Victim(&frame_id);
    if (this->pages_[frame_id].IsDirty()) {
      this->disk_manager_->WritePage(this->pages_[frame_id].GetPageId(), this->pages_[frame_id].GetData());

      // Another way to write back
      // std::mutex mtx;
      // mtx.lock();
      // this->latch_.unlock();
      // this->FlushPgImp(this->pages_[frame_id].GetPageId());  // Because of the latch_, this will be blocked!
      // this->latch_.lock();
      // mtx.unlock();
    }
  }
  this->page_table_.erase(this->pages_[frame_id].GetPageId());
  this->page_table_[*page_id] = frame_id;
  this->pages_[frame_id].page_id_ = *page_id;
  this->pages_[frame_id].is_dirty_ = false;
  this->pages_[frame_id].pin_count_ = 1;
  this->replacer_->Pin(frame_id);
  memset(this->pages_[frame_id].data_, '\0', PAGE_SIZE);
  return &this->pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(this->latch_);
  frame_id_t frame_id = 0;
  if (this->page_table_.find(page_id) != this->page_table_.end()) {
    frame_id = this->page_table_[page_id];
    this->pages_[frame_id].pin_count_ += 1;
    this->replacer_->Pin(frame_id);
    return &this->pages_[frame_id];
  }
  if (!this->free_list_.empty()) {
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else if (this->replacer_->Size() > 0) {
    this->replacer_->Victim(&frame_id);
  } else {
    return nullptr;
  }
  if (this->pages_[frame_id].IsDirty()) {
    this->disk_manager_->WritePage(this->pages_[frame_id].GetPageId(),
                                   static_cast<const char *>(this->pages_[frame_id].data_));
    this->pages_[frame_id].is_dirty_ = false;
  }
  this->page_table_.erase(this->pages_[frame_id].GetPageId());
  this->page_table_[page_id] = frame_id;
  this->pages_[frame_id].page_id_ = page_id;
  this->pages_[frame_id].is_dirty_ = false;
  this->pages_[frame_id].pin_count_ = 1;
  this->replacer_->Pin(frame_id);
  this->disk_manager_->ReadPage(page_id, this->pages_[frame_id].data_);
  return &this->pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->page_table_.find(page_id) == this->page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = this->page_table_[page_id];
  if (this->pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  this->page_table_.erase(this->pages_[frame_id].GetPageId());
  this->pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  this->pages_[frame_id].is_dirty_ = false;
  this->pages_[frame_id].pin_count_ = 0;
  memset(this->pages_[frame_id].data_, sizeof(char), sizeof(this->pages_[frame_id].data_));
  this->page_table_.erase(page_id);
  this->free_list_.push_back(frame_id);
  this->DeallocatePage(page_id);
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->page_table_.find(page_id) == this->page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = this->page_table_[page_id];
  if (is_dirty) {
    this->pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (this->pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  this->pages_[frame_id].pin_count_ -= 1;
  if (this->pages_[frame_id].GetPinCount() == 0) {
    this->replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
