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
      right_preidctor_(nullptr),
      cur_pos_(0) {
  left_predictor_ = plan_->LeftJoinKeyExpression();
  right_preidctor_ = plan_->RightJoinKeyExpression();
  if (left_predictor_ == nullptr) {
    left_predictor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
  if (right_preidctor_ == nullptr) {
    right_preidctor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }

  left_executor_->Init();
  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    const Schema *left_output_schema = plan_->GetLeftPlan()->OutputSchema();

    std::vector<Value> values;
    values.reserve(left_output_schema->GetColumnCount());
    for (const auto &col : left_output_schema->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(&left_tuple, left_output_schema));
    }
    Value val = left_predictor_->Evaluate(&left_tuple, left_output_schema);
    HashJoinKey key{val};
    hash_map_[key].emplace_back(std::move(values));
  }
}

HashJoinExecutor::~HashJoinExecutor() {
  if (left_predictor_ != plan_->LeftJoinKeyExpression()) {
    delete left_predictor_;
  }
  if (right_preidctor_ != plan_->RightJoinKeyExpression()) {
    delete right_preidctor_;
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *right_output_schema = plan_->GetRightPlan()->OutputSchema();
  Value val;
  if (tuple->GetData() != nullptr) {
    val = right_preidctor_->Evaluate(tuple, right_output_schema);
  }
  if (val.GetTypeId() == TypeId::INVALID || cur_pos_ >= hash_map_[HashJoinKey{val}].size()) {
    if (val.GetTypeId() != TypeId::INVALID && cur_pos_ == 0) {
      hash_map_.erase(HashJoinKey{val});
    }
    while (right_executor_->Next(tuple, rid)) {
      val = right_preidctor_->Evaluate(tuple, right_output_schema);
      if (hash_map_.find(HashJoinKey{val}) != hash_map_.end()) {
        cur_pos_ = 0;
        break;
      }
    }
    if (cur_pos_ != 0) {
      return false;
    }
  }
  if (hash_map_.find(HashJoinKey{val}) != hash_map_.end()) {
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());

    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
      if (expr->GetTupleIdx() == 0) {
        values.push_back(hash_map_[HashJoinKey{val}][cur_pos_][expr->GetColIdx()]);
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
