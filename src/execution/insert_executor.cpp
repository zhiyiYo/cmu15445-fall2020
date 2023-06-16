//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_infos_(exec_ctx->GetCatalog()->GetTableIndexes(table_metadata_->name_)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (index_ >= plan_->RawValues().size()) {
      return false;
    }

    *tuple = {plan_->RawValuesAt(index_++), &table_metadata_->schema_};
    InsertTuple(tuple, rid);
    return true;
  } else {
    auto has_data = child_executor_->Next(tuple, rid);
    if (has_data) {
      InsertTuple(tuple, rid);
    }
    return has_data;
  }
}

void InsertExecutor::InsertTuple(Tuple *tuple, RID *rid) {
  // 更新数据表
  table_metadata_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

  // 更新索引
  for (auto &index_info : index_infos_) {
    index_info->index_->InsertEntry(
        tuple->KeyFromTuple(table_metadata_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
