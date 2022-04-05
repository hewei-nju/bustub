//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  std::lock_guard<std::mutex> lock(this->latch_);
  this->buffer_pool_managers_.resize(num_instances);
  for (size_t i = 0; i < num_instances; i++) {
    this->buffer_pool_managers_[i] =
        std::make_unique<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  std::lock_guard<std::mutex> lock(this->latch_);
  size_t result = 0;
  for (const auto &buffer_pool_manager : this->buffer_pool_managers_) {
    result += buffer_pool_manager->GetPoolSize();
  }
  return result;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->buffer_pool_managers_.empty()) {
    return nullptr;
  }
  size_t idx = page_id % buffer_pool_managers_.size();
  return this->buffer_pool_managers_[idx].get();
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(this->latch_);

  if (this->buffer_pool_managers_.empty()) {
    return nullptr;
  }
  BufferPoolManager *buffer_pool_manager = this->buffer_pool_managers_[page_id % this->buffer_pool_managers_.size()].get();
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->buffer_pool_managers_.empty()) {
    return false;
  }
  BufferPoolManager *buffer_pool_manager = this->buffer_pool_managers_[page_id % this->buffer_pool_managers_.size()].get();
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(this->latch_);

  if (this->buffer_pool_managers_.empty()) {
    return false;
  }
  BufferPoolManager *buffer_pool_manager = this->buffer_pool_managers_[page_id % this->buffer_pool_managers_.size()].get();
  return buffer_pool_manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lock(this->latch_);
  for (const auto &buffer_pool_manager : this->buffer_pool_managers_) {
    if (Page *result = buffer_pool_manager->NewPage(page_id); result != nullptr) {
      return result;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(this->latch_);
  if (this->buffer_pool_managers_.empty()) {
    return false;
  }
  BufferPoolManager *buffer_pool_manager = this->buffer_pool_managers_[page_id % this->buffer_pool_managers_.size()].get();
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  std::lock_guard<std::mutex> lock(this->latch_);
  for (const auto &buffer_pool_manager : this->buffer_pool_managers_) {
    buffer_pool_manager->FlushAllPages();
  }
}

}  // namespace bustub
