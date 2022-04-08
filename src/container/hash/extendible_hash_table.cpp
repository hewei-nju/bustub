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
  return static_cast<uint32_t>(this->Hash(key) & dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(static_cast<uint32_t>(this->Hash(key) & dir_page->GetGlobalDepthMask()));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(this->buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(this->buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  this->table_latch_.RLock();
  HashTableDirectoryPage *dir_page = this->FetchDirectoryPage();
  uint32_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> * hash_table_bucket_page = this->FetchBucketPage(bucket_page_id);
  bool ret = hash_table_bucket_page->GetValue(key, this->comparator_, result);
  this->table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.RLock();
  HashTableDirectoryPage *dir_page = this->FetchDirectoryPage();
  uint32_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> * hash_table_bucket_page = this->FetchBucketPage(bucket_page_id);
  bool ret = false;
  if (hash_table_bucket_page->IsFull()) {
    ret = this->SplitInsert(transaction, key, value);
  } else {
    ret = hash_table_bucket_page->Insert(key, value, this->comparator_);
  }
  this->table_latch_.RUnlock();
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
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = this->buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    return ret;
  }

  HashTableDirectoryPage *dir_page = this->FetchDirectoryPage();
  uint32_t bucket_idx = (this->Hash(key) & dir_page->GetGlobalDepthMask());
  page_id_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = this->FetchBucketPage(bucket_page_id);
  // Extend the directory and bucket
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_idx)) {
    dir_page->IncrGlobalDepth();
    for (size_t i = dir_page->Size() / 2 - 1; i >= 0; i--) {
      dir_page->SetBucketPageId(i + (1 << dir_page->GetGlobalDepth()), dir_page->GetBucketPageId(i));
      if (dir_page->GetLocalHighBit(i) != dir_page->GetLocalHighBit(bucket_idx) &&
       dir_page->GetBucketPageId(i) == bucket_page_id) {
         dir_page->SetBucketPageId(i, new_page->GetPageId());
        dir_page->IncrLocalDepth(i);
      }
    }
  } else {
    for (size_t i = dir_page->Size() - 1; i >= 0; i--) {
      if (dir_page->GetBucketPageId(i) == bucket_page_id) {
        dir_page->IncrLocalDepth(i);
        if (dir_page->GetLocalHighBit(i) != dir_page->GetLocalHighBit(bucket_idx)) {
          dir_page->SetBucketPageId(i, new_page->GetPageId());
        }
      }
    }
  }

  std::vector<MappingType> key_values(BUCKET_ARRAY_SIZE);
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    key_values[i].first = bucket_page->KeyAt(i);
    key_values[i].second = bucket_page->ValueAt(i);
    bucket_page->RemoveAt(i);
  }

  for (const auto & [k, v] : key_values) {
    bucket_page_id = this->KeyToPageId(k, dir_page);
    ret = this->FetchBucketPage(bucket_page_id)->Insert(k, v, this->comparator_);
    if (!ret) {
      return ret;
    }
  }
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.RLock();
  bool ret = false;
  HashTableDirectoryPage *dir_page = this->FetchDirectoryPage();
  uint32_t bucket_idx = (this->Hash(key) & dir_page->GetGlobalDepthMask());
  page_id_t bucket_page_id = this->KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = this->FetchBucketPage(bucket_page_id);
  ret = bucket_page->Remove(key, value, this->comparator_);

  bool merge_flag = true;
  if (!bucket_page->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) == 0) {
    merge_flag = false;
  }
  // Splited image
  for (size_t i = 0; i < dir_page->Size() && merge_flag; i++) {
    if (dir_page->GetBucketPageId(i) != bucket_page_id 
      && (i & dir_page->GetLocalDepthMask(i)) == (bucket_idx & dir_page->GetLocalDepthMask(bucket_idx))) {
        break;
      }
  }

  if (merge_flag) {
    this->Merge(transaction, key, value);
  }
  this->table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = this->FetchDirectoryPage();
  page_id_t bucket_page_id = this->KeyToPageId(key, dir_page);
  uint32_t image_bucket_idx = 0;
  for (size_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      uint32_t local_high_bit = dir_page->GetLocalHighBit(i);
      if (local_high_bit != 0) {
        image_bucket_idx = i - local_high_bit;
      } else {
        image_bucket_idx = i + local_high_bit;
      }
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(image_bucket_idx));
      dir_page->DecrLocalDepth(i);
      dir_page->DecrLocalDepth(image_bucket_idx);
    }
  }
  this->buffer_pool_manager_->DeletePage(bucket_page_id);

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
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
