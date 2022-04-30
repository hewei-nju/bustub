//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // check the 2 PL state
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // check the txn's isolation level
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  // put the txn into the request queue
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  // block if there has an exclusive lock in rid
  if (lock_request_queue.exclusive_) {
    lock_request_queue.cv_.wait(
        lock, [&]() -> bool { return txn->GetState() == TransactionState::ABORTED || !lock_request_queue.exclusive_; });
  }

  // add a shared lock
  txn->GetSharedLockSet()->emplace(rid);
  lock_request_queue.shared_count_++;
  auto iter = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockRequest &lock_request) -> bool { return lock_request.txn_id_ == txn->GetTransactionId(); });
  assert(iter != lock_request_queue.request_queue_.end());
  iter->granted_ = true;

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // check the 2 PL state
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // put the txn into the request queue
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  // block if there has an exclusive lock in rid
  if (lock_request_queue.exclusive_ || lock_request_queue.shared_count_ > 0) {
    lock_request_queue.cv_.wait(lock, [&]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             !(lock_request_queue.exclusive_ || lock_request_queue.shared_count_ > 0);
    });
  }

  // add an exclusive lock
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_request_queue.exclusive_ = true;
  auto iter = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockRequest &lock_request) -> bool { return lock_request.txn_id_ == txn->GetTransactionId(); });
  assert(iter != lock_request_queue.request_queue_.end());
  iter->granted_ = true;

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // check the 2 PL state
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // check there if has the txn waiting for lock upgrade
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  // update the status of rid's lock request queue
  lock_request_queue.upgrading_ = txn->GetTransactionId();

  // block if there has an exclusive lock or other shared lock in rid
  if (lock_request_queue.exclusive_ || lock_request_queue.shared_count_ > 1) {
    lock_request_queue.cv_.wait(lock, [&]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             !(lock_request_queue.exclusive_ || lock_request_queue.shared_count_ > 1);
    });
  }

  // erase the shared lock and an exclusive lock
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_request_queue.exclusive_ = true;
  auto iter = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockRequest &lock_request) -> bool { return lock_request.txn_id_ == txn->GetTransactionId(); });
  assert(iter != lock_request_queue.request_queue_.end());

  // update the status of rid's lock request queue and lock mode
  lock_request_queue.upgrading_ = INVALID_TXN_ID;
  iter->lock_mode_ = LockMode::EXCLUSIVE;

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // erase the lock of the txn for rid
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // update the status of rid's lock request queue
  auto iter = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockRequest &lock_request) -> bool { return lock_request.txn_id_ == txn->GetTransactionId(); });
  assert(iter != lock_request_queue.request_queue_.end());
  lock_request_queue.exclusive_ = false;
  if (iter->lock_mode_ == LockMode::SHARED) {
    lock_request_queue.shared_count_--;
  }

  // check the status of txn: Growing State, READ_COMMITTED, Shared Lock
  if (!(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        iter->lock_mode_ == LockMode::SHARED)) {
    if (txn->GetState() != TransactionState::ABORTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  // notify other block txn in the lock request queue
  if (iter->lock_mode_ == LockMode::EXCLUSIVE ||
      (iter->lock_mode_ == LockMode::SHARED && lock_request_queue.shared_count_ == 0)) {
    lock_request_queue.request_queue_.erase(iter);
    lock_request_queue.cv_.notify_all();
  } else {
    lock_request_queue.request_queue_.erase(iter);
  }

  return true;
}

}  // namespace bustub
