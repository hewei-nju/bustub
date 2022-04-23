//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      left_predictor_(nullptr),
      right_predictor_(nullptr),
      cur_pos_(0), val_({}) {
  left_predictor_ = plan_->LeftJoinKeyExpression();
  right_predictor_ = plan_->RightJoinKeyExpression();
  if (left_predictor_ == nullptr) {
    left_predictor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
  if (right_predictor_ == nullptr) {
    right_predictor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }

  left_executor_->Init();
  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    const Schema *left_output_schema = left_executor_->GetOutputSchema();

    std::vector<Value> values;
    values.reserve(left_output_schema->GetColumnCount());
    for (uint32_t i = 0; i < left_output_schema->GetColumnCount(); i++) {
      values.emplace_back(left_tuple.GetValue(left_output_schema, i));
    }
    Value val = left_predictor_->Evaluate(&left_tuple, left_output_schema);
    hash_map_[HashJoinKey{val}].emplace_back(std::move(values));
  }
}

HashJoinExecutor::~HashJoinExecutor() {
  if (left_predictor_ != plan_->LeftJoinKeyExpression()) {
    delete left_predictor_;
  }
  if (right_predictor_ != plan_->RightJoinKeyExpression()) {
    delete right_predictor_;
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *right_output_schema = right_executor_->GetOutputSchema();

  if (val_.GetTypeId() == TypeId::INVALID || hash_map_.find(HashJoinKey{val_}) == hash_map_.end()
    || cur_pos_ >= hash_map_[HashJoinKey{val_}].size()) {
    while (right_executor_->Next(tuple, rid)) {
      val_ = right_predictor_->Evaluate(tuple, right_output_schema);
      if (hash_map_.find(HashJoinKey{val_}) != hash_map_.end()) {
        cur_pos_ = 0;
        break;
      }
    }
    if (cur_pos_ != 0) {
      return false;
    }
  }

  if (val_.GetTypeId() != TypeId::INVALID && hash_map_.find(HashJoinKey{val_}) != hash_map_.end()) {
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());

    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
      if (expr->GetTupleIdx() == 0) {
        values.push_back(hash_map_[HashJoinKey{val_}][cur_pos_][expr->GetColIdx()]);
      } else {
        values.push_back(tuple->GetValue(right_output_schema, expr->GetColIdx()));
      }
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    cur_pos_++;
    return true;
  }
  return false;
}

}  // namespace bustub
