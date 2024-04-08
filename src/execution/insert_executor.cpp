//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

    InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
            : AbstractExecutor(exec_ctx), plan_{plan}, childExecutor{std::move(child_executor)} {
        this->tableInfo = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    }

    void InsertExecutor::Init() {
        childExecutor->Init();
        try {
            bool is_locked = exec_ctx_->GetLockManager()->LockTable(
                    exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, tableInfo->oid_);
            if (!is_locked)
                throw ExecutionException("Insert Executor Get Table Lock Failed");
        } catch (TransactionAbortException e) {
            throw ExecutionException("Insert Executor Get Table Lock Failed");
        }
        tableIndexes = exec_ctx_->GetCatalog()->GetTableIndexes(tableInfo->name_);
    }

    auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        if (isEnd)
            return false;
        Tuple toInsertTuple{};
        RID emitRid;
        int32_t insertCount = 0;
        while (childExecutor->Next(&toInsertTuple, &emitRid)) {
            bool inserted = tableInfo->table_->InsertTuple(toInsertTuple, rid, exec_ctx_->GetTransaction());
            if (inserted) {
                try {
                    bool is_locked = exec_ctx_->GetLockManager()->LockRow(
                            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, tableInfo->oid_, *rid);
                    if (!is_locked)
                        throw ExecutionException("Insert Executor Get Row Lock Failed");
                } catch (TransactionAbortException e) {
                    throw ExecutionException("Insert Executor Get Row Lock Failed");
                }
                std::for_each(tableIndexes.begin(), tableIndexes.end(),
                              [&toInsertTuple, &rid, &table_info = tableInfo, &exec_ctx = exec_ctx_](IndexInfo *index) {
                                  index->index_->InsertEntry(toInsertTuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                                          index->index_->GetKeyAttrs()),
                                                             *rid, exec_ctx->GetTransaction());
                              });
                insertCount++;
            }
        }
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        values.emplace_back(TypeId::INTEGER, insertCount);
        *tuple = Tuple{values, &GetOutputSchema()};
        isEnd = true;
        return true;
    }
}  // namespace bustub
