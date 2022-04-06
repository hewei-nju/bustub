//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include <bitset>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if ((readable_[i / 8] & mask[i % 8]) != 0 && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  size_t idx = BUCKET_ARRAY_SIZE;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if ((readable_[i / 8] & mask[i % 8]) != 0 && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
      return false;
    }
    if ((readable_[i / 8] & mask[i % 8]) == 0 && idx == BUCKET_ARRAY_SIZE) {
      idx = i;
    }
  }
  if (idx < BUCKET_ARRAY_SIZE) {
    array_[idx] = {key, value};
    occupied_[idx / 8] |= mask[idx % 8];
    readable_[idx / 8] |= mask[idx % 8];
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  const uint32_t rmask[8] = {127, 191, 223, 239, 247, 251, 253, 254};
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if ((readable_[i / 8] & mask[i % 8]) && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
      readable_[i / 8] &= rmask[i % 8];
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  const uint32_t rmask[8] = {127, 191, 223, 239, 247, 251, 253, 254};
  readable_[bucket_idx / 8] &= rmask[bucket_idx % 8];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  return (occupied_[bucket_idx / 8] & mask[bucket_idx % 8]) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  occupied_[bucket_idx / 8] |= mask[bucket_idx % 8];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  return (occupied_[bucket_idx / 8] & mask[bucket_idx % 8]) != 0 &&
         (readable_[bucket_idx / 8] & mask[bucket_idx % 8]) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  const uint32_t mask[8] = {128, 64, 32, 16, 8, 4, 2, 1};
  readable_[bucket_idx / 8] |= mask[bucket_idx % 8];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  int result = 0;
  for (const auto &occupied : occupied_) {
    result += std::bitset<32>(occupied).count();
  }
  return result == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t result = 0;
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; i++) {
    result += std::bitset<32>(readable_[i] & occupied_[i]).count();
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  bool result = true;
  for (const auto &occupied : occupied_) {
    if (occupied != 0) {
      result = false;
      break;
    }
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
