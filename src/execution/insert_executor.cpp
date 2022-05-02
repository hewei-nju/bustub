//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), cur_pos_(0) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool ret = false;

  // Raw Insert
  if (plan_->IsRawInsert()) {
    if (cur_pos_ < plan_->RawValues().size()) {
      *tuple = Tuple(plan_->RawValuesAt(cur_pos_++), &(table_info_->schema_));
      ret = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  } else {
    // Tuple pulled from child executor (at most one)
    if (child_executor_->Next(tuple, rid)) {
      ret = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  }

  if (ret) {
    // try to get a exclusive lock to add the insert record
    if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      return false;
    }

    // Update indexs
    for (auto &index_info : index_infos_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  
    // Unlock if isolation level is not REPEATABLE_READ
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), *rid);
    }
  }

  return ret;
}

}  // namespace bustub
