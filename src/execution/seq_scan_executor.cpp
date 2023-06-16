//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())) {}

void SeqScanExecutor::Init() { it_ = table_metadata_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();

  while (it_ != table_metadata_->table_->End()) {
    *tuple = *it_++;
    *rid = tuple->GetRid();

    if (!predicate || predicate->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      // 只保留输出列
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->Evaluate(tuple, &table_metadata_->schema_));
      }

      *tuple = {values, GetOutputSchema()};
      return true;
    }
  }

  return false;
}

}  // namespace bustub
