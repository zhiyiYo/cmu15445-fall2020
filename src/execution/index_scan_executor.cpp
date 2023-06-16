//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
      index_(dynamic_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info_->index_.get())),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)) {}

void IndexScanExecutor::Init() { it_ = index_->GetBeginIterator(); }

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();

  while (it_ != index_->GetEndIterator()) {
    *rid = (*it_).second;
    table_metadata_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++it_;

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
