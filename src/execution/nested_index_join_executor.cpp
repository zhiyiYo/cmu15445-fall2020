//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetInnerTableOid())),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexName(), inner_table_info_->name_)) {}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID left_rid, right_rid;

  auto left_schema = plan_->OuterTableSchema();
  auto right_schema = plan_->InnerTableSchema();

  while (true) {
    if (!inner_result_.empty()) {
      right_rid = inner_result_.back();
      inner_result_.pop_back();
      inner_table_info_->table_->GetTuple(right_rid, &right_tuple, exec_ctx_->GetTransaction());

      // 拼接 tuple
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema));
      }

      *tuple = {values, GetOutputSchema()};
      return true;
    }

    if (!child_executor_->Next(&left_tuple_, &left_rid)) {
      return false;
    }

    // 在内表的索引上寻找匹配值列表
    auto value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
    auto inner_key = Tuple({value}, index_info_->index_->GetKeySchema());
    index_info_->index_->ScanKey(inner_key, &inner_result_, exec_ctx_->GetTransaction());
  }

  return false;
}

}  // namespace bustub
