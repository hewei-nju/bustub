//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predictor_(nullptr) {}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor() {
  if (predictor_ != plan_->Predicate()) {
    delete predictor_;
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  // Nested execution, prevent memory leak
  if (predictor_ != plan_->Predicate()) {
    delete predictor_;
  }
  predictor_ = plan_->Predicate();
  if (predictor_ == nullptr) {
    predictor_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;

  while (!(left_rid_ == RID{}) || left_executor_->Next(&left_tuple_, &left_rid_)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      Value val = predictor_->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                           plan_->GetRightPlan()->OutputSchema());
      if (val.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema()->GetColumnCount());
        for (const auto &col : plan_->OutputSchema()->GetColumns()) {
          auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
          if (expr->GetTupleIdx() == 0) {
            values.push_back(left_tuple_.GetValue(plan_->GetLeftPlan()->OutputSchema(), expr->GetColIdx()));
          } else {
            values.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), expr->GetColIdx()));
          }
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        *rid = left_rid_;
        return true;
      }
    }
    left_rid_ = RID{};
    right_executor_->Init();
  }

  return false;
}

}  // namespace bustub
