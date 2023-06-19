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

void SeqScanExecutor::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->Unlock(txn, rid);
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->GetPredicate();
  auto txn = exec_ctx_->GetTransaction();

  while (it_ != table_metadata_->table_->End()) {
    *rid = it_->GetRid();

    // 上锁
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !txn->IsExclusiveLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockShared(txn, *rid);
    }

    *tuple = *it_++;

    if (!predicate || predicate->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      // 只保留输出列
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->Evaluate(tuple, &table_metadata_->schema_));
      }

      *tuple = {values, GetOutputSchema()};

      // 解锁
      Unlock(txn, *rid);
      return true;
    }

    Unlock(txn, *rid);
  }

  return false;
}

}  // namespace bustub
