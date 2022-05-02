//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
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

    Tuple dest_tuple = GenerateUpdatedTuple(*tuple);
    ret = table_info_->table_->UpdateTuple(dest_tuple, *rid, exec_ctx_->GetTransaction());
    if (ret) {
      for (auto &index_info : index_infos_) {
        Tuple src_key =
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        Tuple dest_key =
            dest_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(src_key, *rid, exec_ctx_->GetTransaction());
        index_info->index_->InsertEntry(dest_key, *rid, exec_ctx_->GetTransaction());
        IndexWriteRecord index_write_record = {*rid,       table_info_->oid_,      WType::UPDATE,
                                               dest_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
        index_write_record.old_tuple_ = *tuple;
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_write_record);
      }
    }
  }

  return ret;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
