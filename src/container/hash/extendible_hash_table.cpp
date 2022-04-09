//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  // New a directory page
  Page *new_dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  if (new_dir_page == nullptr) {
    exit(EXIT_FAILURE);
  }
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(new_dir_page);

  // New a bucket page
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  Page *new_bucket_page = buffer_pool_manager_->NewPage(&bucket_page_id);
  if (new_bucket_page == nullptr) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->DeletePage(directory_page_id_);
    exit(EXIT_FAILURE);
  }
  dir_page->SetBucketPageId(0, bucket_page_id);
  dir_page->SetLocalDepth(0, 0);

  // Unpin directory page and bucket page
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return static_cast<uint32_t>(Hash(key) & dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(static_cast<uint32_t>(Hash(key) & dir_page->GetGlobalDepthMask()));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  // Fetch directory page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // Fetch bucket page
  uint32_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  // Get value
  bool ret = hash_table_bucket_page->GetValue(key, comparator_, result);

  // Unpin directory page and bucket page
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  // Fetch directory page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // Fetch bucket page
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  // Insert key and value
  bool ret = false;
  if (hash_table_bucket_page->IsFull()) {
    // Unpin directory page and bucket page
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);

    ret = SplitInsert(transaction, key, value);
  } else {
    // Insert key and value
    ret = hash_table_bucket_page->Insert(key, value, comparator_);

    // Unpin directory page and bucket page
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  }
  table_latch_.WUnlock();
  return ret;
}

/**
 * Split the bucket
 * 1. if the bucket local depth == the directory global depth
 * 1.1 extend the directory, then extend the bucket
 * 2. if the bucket local depth != the drectory global depth
 * 2.1 just need extend the bucket
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;

  // New bucket page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    return ret;
  }

  // Fetch directory page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // Fetch bucket page
  uint32_t bucket_idx = (this->Hash(key) & dir_page->GetGlobalDepthMask());
  page_id_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // Extend the directory and bucket
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    if (dir_page->Size() == DIRECTORY_ARRAY_SIZE) {
      return ret;
    }

    // Extend directory and bucket
    for (uint32_t i = dir_page->Size() - 1; i >= 0; i--) {
      if (dir_page->Size() == 1) {
        dir_page->SetBucketPageId(1, new_page_id);
        dir_page->IncrLocalDepth(0);
        dir_page->IncrLocalDepth(1);
        break;
      }
      dir_page->SetBucketPageId(i + (1 << dir_page->GetGlobalDepth()), dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(i + (1 << dir_page->GetGlobalDepth()), dir_page->GetLocalDepth(i));
      if (dir_page->GetBucketPageId(i) == bucket_page_id) {
        uint32_t new_page_idx = i + (1 << dir_page->GetGlobalDepth());
        dir_page->SetBucketPageId(new_page_idx, new_page_id);
        dir_page->IncrLocalDepth(i);
        dir_page->IncrLocalDepth(new_page_idx);
      }

      if (i == 0) {
        break;
      }
    }
    dir_page->IncrGlobalDepth();
  } else {
    // Extend bucket
    uint32_t local_high_bit = dir_page->GetLocalHighBit(bucket_idx);
    for (uint32_t i = dir_page->Size() - 1; i >= 0; i--) {
      if (dir_page->GetBucketPageId(i) == bucket_page_id) {
        if ((dir_page->GetLocalHighBit(i) & i) != (local_high_bit & bucket_idx)) {
          dir_page->SetBucketPageId(i, new_page_id);
        }
        dir_page->IncrLocalDepth(i);
      }

      if (i == 0) {
        break;
      }
    }
  }

  // Collect all the key and value in the bucket page
  std::vector<MappingType> key_values(BUCKET_ARRAY_SIZE + 1);
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    key_values[i].first = bucket_page->KeyAt(i);
    key_values[i].second = bucket_page->ValueAt(i);
    bucket_page->RemoveAt(i);
  }
  // Collect new key and value
  key_values.back() = {key, value};

  // Reinsert key and value into bucket
  for (const auto &[k, v] : key_values) {
    page_id_t insert_page_id = KeyToPageId(k, dir_page);
    if (insert_page_id == bucket_page_id) {
      ret = bucket_page->Insert(k, v, comparator_);
    } else if (insert_page_id == new_page_id) {
      ret = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page)->Insert(k, v, comparator_);
    } else {
      ret = false;
    }
    if (!ret) {
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      buffer_pool_manager_->UnpinPage(new_page_id, true);
      return ret;
    }
  }

  // Unpin directory page, bucket page, new page
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  ret = true;
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.WLock();
  bool ret = false;

  // Fetch directory page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // Fetch bucket page
  uint32_t bucket_idx = (this->Hash(key) & dir_page->GetGlobalDepthMask());
  page_id_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // Remove key and value
  ret = bucket_page->Remove(key, value, this->comparator_);

  bool merge_flag = true;
  if (!bucket_page->IsEmpty() || dir_page->GetLocalDepth(bucket_idx) == 0) {
    merge_flag = false;
  }

  // Splited image
  uint32_t local_high_bit = dir_page->GetLocalHighBit(bucket_idx) >> 1;
  for (uint32_t i = 0; i < dir_page->Size() && merge_flag; i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      uint32_t image_bucket_idx = 0;
      uint32_t local_high_val = i & local_high_bit;
      if (local_high_val != 0) {
        image_bucket_idx = i - local_high_bit;
      } else {
        image_bucket_idx = i + local_high_bit;
      }
      if (dir_page->GetLocalDepth(image_bucket_idx) != dir_page->GetLocalDepth(bucket_idx)) {
        merge_flag = false;
      }
    }
  }

  // Unpin directory page and bucket page
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);

  if (merge_flag) {
    // Merge image bucket
    Merge(transaction, key, value);
  }

  table_latch_.WUnlock();
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Fetch directory page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  uint32_t bucket_idx = (Hash(key) & dir_page->GetGlobalDepthMask());
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);

  uint32_t local_high_bit = dir_page->GetLocalHighBit(bucket_idx) >> 1;
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      uint32_t image_bucket_idx = 0;
      uint32_t local_high_val = i & local_high_bit;
      if (local_high_val != 0) {
        image_bucket_idx = i - local_high_bit;
      } else {
        image_bucket_idx = i + local_high_bit;
      }
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(image_bucket_idx));
      dir_page->DecrLocalDepth(i);
      dir_page->DecrLocalDepth(image_bucket_idx);
    }
  }
  buffer_pool_manager_->DeletePage(bucket_page_id);

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
