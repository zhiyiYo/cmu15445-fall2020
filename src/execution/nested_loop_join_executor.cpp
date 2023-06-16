//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
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
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID left_rid;
  is_done_ = !left_executor_->Next(&left_tuple_, &left_rid);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid, left_rid;
  auto predicate = plan_->Predicate();
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();

  while (!is_done_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (!predicate || predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        // 拼接 tuple
        std::vector<Value> values;
        for (auto &col : GetOutputSchema()->GetColumns()) {
          values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema));
        }

        *tuple = {values, GetOutputSchema()};
        return true;
      }
    }

    is_done_ = !left_executor_->Next(&left_tuple_, &left_rid);
    right_executor_->Init();
  }

  return false;
}

}  // namespace bustub
