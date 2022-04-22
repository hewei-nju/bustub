//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
        aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {
        std::vector<Value> keys = aht_.GenerateInitialAggregateValue().aggregates_;
        const std::vector<const AbstractExpression *> &exprs =  plan_->GetAggregates();
        child_->Init();

        Tuple tuple;
        RID rid;
        while (child_->Next(&tuple, &rid)) {
            if (!plan_->GetGroupBys().empty()) {
                // Group by
                keys.clear();
                keys.reserve(plan_->GetGroupBys().size());
                for (const auto &by : plan_->GetGroupBys()) {
                    keys.emplace_back(by->Evaluate(&tuple, child_->GetOutputSchema()));
                }
            }

            std::vector<Value> values;
            values.reserve(exprs.size());
            for (const auto &expr : exprs) {
                values.emplace_back(expr->Evaluate(&tuple, child_->GetOutputSchema()));
            }
            aht_.InsertCombine(AggregateKey{keys}, AggregateValue{values});
        }
    }

void AggregationExecutor::Init() {
    aht_iterator_ = aht_.Begin();
    child_->Init();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
    while (aht_iterator_ != aht_.End()) {
        if (plan_->GetHaving() != nullptr) {
            // Having
            Value val = plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_);
            if (!val.GetAs<bool>()) {
                ++aht_iterator_;
                continue;
            }
        }

        std::vector<Value> values;
        values.reserve(plan_->OutputSchema()->GetColumnCount());
        for (const auto &col : plan_->OutputSchema()->GetColumns()) {
            auto expr = reinterpret_cast<const AggregateValueExpression *>(col.GetExpr());
            values.emplace_back(expr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
        }
        ++aht_iterator_;
        *tuple = Tuple(values, plan_->OutputSchema());        
        return true;
    }
    return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
