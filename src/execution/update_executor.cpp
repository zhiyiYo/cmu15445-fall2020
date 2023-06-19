//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)),
      index_infos_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  // 更新数据表
  auto new_tuple = GenerateUpdatedTuple(*tuple);

  // 加锁
  auto txn = exec_ctx_->GetTransaction();
  if (txn->IsSharedLocked(*rid)) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, *rid);
  } else {
    exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);
  }

  table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());

  // 更新索引
  for (auto &index_info : index_infos_) {
    // 删除旧的 tuple
    index_info->index_->DeleteEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());

    // 插入新的 tuple
    index_info->index_->InsertEntry(
        new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());

    IndexWriteRecord record(*rid, table_info_->oid_, WType::UPDATE, *tuple, index_info->index_oid_,
                            exec_ctx_->GetCatalog());
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(record);
  }


  return true;
}
}  // namespace bustub
