//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

    SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
            : AbstractExecutor(exec_ctx), plan_(plan) {
        this->tableInfo = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    }

    void SeqScanExecutor::Init() {
        if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            try {
                bool is_locked = exec_ctx_->GetLockManager()->LockTable(
                        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, tableInfo->oid_);
                if (!is_locked)
                    throw ExecutionException("SeqScan Executor Get Table Lock Failed");
            } catch (TransactionAbortException e) {
                throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
            }
        }
        this->tableIter = tableInfo->table_->Begin(exec_ctx_->GetTransaction());
    }

    auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
        do {
            if (tableIter == tableInfo->table_->End()) {
                if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
                    const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(tableInfo->oid_);
                    table_oid_t oid = tableInfo->oid_;
                    for (auto ridTmp : locked_row_set) {
                        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, ridTmp);
                    }
                    exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), tableInfo->oid_);
                }
                return false;
            }
            *tuple = *tableIter;
            *rid = tuple->GetRid();
            ++tableIter;
        } while (plan_->filter_predicate_ != nullptr &&
                 !plan_->filter_predicate_->Evaluate(tuple, tableInfo->schema_).GetAs<bool>());
        if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            try {
                bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                                      tableInfo->oid_, *rid);
                if (!is_locked)
                    throw ExecutionException("SeqScan Executor Get Table Lock Failed");
            } catch (TransactionAbortException e) {
                throw ExecutionException("SeqScan Executor Get Row Lock Failed");
            }
        }
        return true;
    }

}  // namespace bustub
