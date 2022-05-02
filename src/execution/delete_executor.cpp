//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool ret = false;
  if (child_executor_->Next(tuple, rid)) {
    // Check the tuple is already has a shared lock and then try to upgrade to exclusive lock
     if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
        exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
        return false;
      }
    } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(*rid)) {
      if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
        exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
        return false;
      }
    }
    ret = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
  }

  if (ret) {
    // Update indexs
    for (auto &index_info : index_infos_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    // Unlock if isolation level is not REPEATABLE_READ
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), *rid);
    }
  }

  return ret;
}

}  // namespace bustub
