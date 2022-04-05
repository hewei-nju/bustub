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
  this->bufferPoolManagers_.resize(num_instances);
  for (size_t i = 0; i < num_instances; i++) {
    this->bufferPoolManagers_[i] =
        std::make_unique<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  size_t result = 0;
  for (const auto &instance : this->bufferPoolManagers_) {
    result += instance->GetPoolSize();
  }
  return result;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  if (this->bufferPoolManagers_.empty()) {
    return nullptr;
  }
  size_t idx = page_id % bufferPoolManagers_.size();
  return this->bufferPoolManagers_[idx].get();
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  if (this->bufferPoolManagers_.empty()) {
    return nullptr;
  }
  BufferPoolManager *bufferPoolManager = this->GetBufferPoolManager(page_id);
  return bufferPoolManager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  if (this->bufferPoolManagers_.empty()) {
    return false;
  }
  BufferPoolManager *bufferPoolManager = this->GetBufferPoolManager(page_id);
  return bufferPoolManager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  if (this->bufferPoolManagers_.empty()) {
    return false;
  }
  BufferPoolManager *bufferPoolManager = this->GetBufferPoolManager(page_id);
  return bufferPoolManager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  for (const auto &bufferPoolManager : this->bufferPoolManagers_) {
    if (Page *result = bufferPoolManager->NewPage(page_id); result != nullptr) {
      return result;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  if (this->bufferPoolManagers_.empty()) {
    return false;
  }
  BufferPoolManager *bufferPoolManager = this->GetBufferPoolManager(page_id);
  return bufferPoolManager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (const auto &bufferPoolManager : this->bufferPoolManagers_) {
    bufferPoolManager->FlushAllPages();
  }
}

}  // namespace bustub
