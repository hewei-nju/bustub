//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID{}, nullptr), predictor_(nullptr) {}

SeqScanExecutor::~SeqScanExecutor() {
  if (predictor_ != plan_->GetPredicate()) {
    delete predictor_;
  }
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());

  // Nested execution, prevent memory leak
  if (predictor_ != plan_->GetPredicate()) {
    delete predictor_;
  }

  predictor_ = plan_->GetPredicate();
  if (predictor_ == nullptr) {
    predictor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_info_->table_->End()) {
    // According to the isolation level to decide to get a lock
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        if (!exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), iter_->GetRid())) {
          exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
          return false;
        }
        break;
    }

    TableIterator cur = iter_++;
    Value val = predictor_->Evaluate(&(*cur), &table_info_->schema_);
    if (val.GetAs<bool>()) {
      const Schema *output_schema = plan_->OutputSchema();

      std::vector<Value> values;
      values.reserve(output_schema->GetColumnCount());
      for (const auto &col : output_schema->GetColumns()) {
        values.emplace_back(col.GetExpr()->Evaluate(&(*cur), &(table_info_->schema_)));
      }

      *tuple = Tuple(values, output_schema);
      *rid = cur->GetRid();

      // if there has a shared lock in IsolationLevel::READ_COMMITTED
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), cur->GetRid());
      }
      return true;
    }

    // if there has a shared lock in IsolationLevel::READ_COMMITTED
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), cur->GetRid());
    }
  }
  return false;
}

}  // namespace bustub
