//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_infos_(exec_ctx->GetCatalog()->GetTableIndexes(table_metadata_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  // 加锁
  auto txn = exec_ctx_->GetTransaction();
  if (txn->IsSharedLocked(*rid)) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, *rid);
  } else {
    exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);
  }

  table_metadata_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());

  // 更新索引
  for (auto &index_info : index_infos_) {
    index_info->index_->DeleteEntry(
        tuple->KeyFromTuple(table_metadata_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());

    IndexWriteRecord record(*rid, table_metadata_->oid_, WType::DELETE, *tuple, index_info->index_oid_,
                            exec_ctx_->GetCatalog());
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(record);
  }

  return true;
}

}  // namespace bustub
