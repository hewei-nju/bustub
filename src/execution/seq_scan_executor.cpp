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
      return true;
    }
  }
  return false;
}

}  // namespace bustub
