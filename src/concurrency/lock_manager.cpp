//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 收缩阶段不允许上锁
  CheckShrinking(txn);

  // 不需要重复上锁
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  auto txn_id = txn->GetTransactionId();

  // 读未提交不需要加读锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // 创建一个加锁请求
  auto &queue = lock_table_[rid];
  auto &request = queue.request_queue_.emplace_back(txn_id, LockMode::SHARED);

  // 没拿到锁就进入阻塞状态
  LOG_INFO("事务 %u 试图在 %u 上加读锁", txn_id, rid.GetSlotNum());
  queue.cv_.wait(lock, [&] { return !queue.writer_enter_ || txn->IsAborted(); });
  LOG_INFO("事务 %u 成功在 %u 上加读锁", txn_id, rid.GetSlotNum());

  // 死锁会导致事务中止
  CheckAborted(txn);

  // 更新锁请求的状态
  queue.reader_count_++;
  request.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  CheckShrinking(txn);

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 创建加锁请求
  auto &queue = lock_table_[rid];
  auto &request = queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  // 没有拿到写锁就进入阻塞状态
  LOG_INFO("事务 %u 试图在 %u 上加写锁", txn->GetTransactionId(), rid.GetSlotNum());
  queue.cv_.wait(lock, [&] { return (!queue.writer_enter_ && queue.reader_count_ == 0) || txn->IsAborted(); });
  LOG_INFO("    事务 %u 成功在 %u 上加写锁", txn->GetTransactionId(), rid.GetSlotNum());

  // 死锁会导致事务中止
  CheckAborted(txn);

  queue.writer_enter_ = true;
  request.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  txn->GetSharedLockSet()->erase(rid);
  auto &queue = lock_table_[rid];
  queue.reader_count_--;

  auto request_it = GetRequest(txn->GetTransactionId(), rid);
  request_it->lock_mode_ = LockMode::EXCLUSIVE;
  request_it->granted_ = false;

  // 如果前面有正在排队升级锁的事务就直接返回
  if (queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  queue.upgrading_ = true;
  LOG_INFO("事务 %u 试图在 %u 上升级为写锁", txn->GetTransactionId(), rid.GetSlotNum());
  queue.cv_.wait(lock, [&] { return (!queue.writer_enter_ && queue.reader_count_ == 0) || txn->IsAborted(); });
  LOG_INFO("    事务 %u 成功在 %u 上升级为写锁", txn->GetTransactionId(), rid.GetSlotNum());

  // 死锁会导致事务中止
  CheckAborted(txn);

  queue.upgrading_ = false;
  queue.writer_enter_ = true;
  request_it->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto request_it = GetRequest(txn->GetTransactionId(), rid);
  auto lock_mode = request_it->lock_mode_;

  // 更新事务状态，读已提交不需要两阶段锁机制
  if (txn->GetState() == TransactionState::GROWING &&
      !(lock_mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 从加锁请求队列中移除事务的请求
  auto &queue = lock_table_[rid];
  queue.request_queue_.erase(request_it);
  LOG_INFO("事务 %u 成功在 %u 上释放锁", txn->GetTransactionId(), rid.GetSlotNum());

  if (lock_mode == LockMode::SHARED) {
    // 唤醒等待读锁的线程
    if (--queue.reader_count_ == 0) {
      queue.cv_.notify_all();
    }
  } else {
    // 唤醒等待写锁的线程
    queue.writer_enter_ = false;
    queue.cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txns_.insert(t1);
  txns_.insert(t2);

  auto &neighbors = waits_for_[t1];
  auto it = std::find(neighbors.begin(), neighbors.end(), t2);
  if (it == neighbors.end()) {
    neighbors.push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &neighbors = waits_for_[t1];
  auto it = std::find(neighbors.begin(), neighbors.end(), t2);
  if (it != neighbors.end()) {
    neighbors.erase(it);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  for (auto &t1 : txns_) {
    DFS(t1);
    if (has_cycle_) {
      *txn_id = *on_stack_txns_.rbegin();
      on_stack_txns_.clear();
      has_cycle_ = false;
      return true;
    }
  }

  on_stack_txns_.clear();
  return false;
}

void LockManager::DFS(txn_id_t txn_id) {
  if (has_cycle_) {
    return;
  }

  on_stack_txns_.insert(txn_id);
  auto &neighbors = waits_for_[txn_id];
  std::sort(neighbors.begin(), neighbors.end());

  for (auto t2 : neighbors) {
    if (!on_stack_txns_.count(t2)) {
      DFS(t2);
    } else {
      has_cycle_ = true;
      return;
    }
  }

  on_stack_txns_.erase(txn_id);
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;

  for (auto &[t1, neighbors] : waits_for_) {
    for (auto t2 : neighbors) {
      edges.emplace_back(t1, t2);
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);

      // 构建等待图
      for (auto &[rid, queue] : lock_table_) {
        std::vector<txn_id_t> grants;

        auto it = queue.request_queue_.begin();
        while (it != queue.request_queue_.end() && it->granted_) {
          grants.push_back(it->txn_id_);
          it++;
        }

        while (it != queue.request_queue_.end()) {
          for (auto &t2 : grants) {
            AddEdge(it->txn_id_, t2);
          }

          wait_rids_[it->txn_id_] = rid;
          it++;
        }
      }

      // 移除环中 id 最小的事务
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        AbortTransaction(txn_id);
      }

      // 清空图
      waits_for_.clear();
      wait_rids_.clear();
      txns_.clear();
    }
  }
}

void LockManager::CheckShrinking(Transaction *txn) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
}

void LockManager::CheckAborted(Transaction *txn) {
  if (txn->IsAborted()) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
}

void LockManager::AbortTransaction(txn_id_t txn_id) {
  auto txn = TransactionManager::GetTransaction(txn_id);
  txn->SetState(TransactionState::ABORTED);
  waits_for_.erase(txn_id);

  // 释放所有 txn 持有的写锁
  for (auto &rid : *txn->GetExclusiveLockSet()) {
    for (auto &req : lock_table_[rid].request_queue_) {
      if (!req.granted_) {
        RemoveEdge(req.txn_id_, txn_id);
      }
    }
  }

  // 释放所有 txn 持有的读锁
  for (auto &rid : *txn->GetSharedLockSet()) {
    for (auto &req : lock_table_[rid].request_queue_) {
      if (!req.granted_) {
        RemoveEdge(req.txn_id_, txn_id);
      }
    }
  }

  // 通知 txn 所在线程事务被终止了
  lock_table_[wait_rids_[txn_id]].cv_.notify_all();
}

std::list<LockManager::LockRequest>::iterator LockManager::GetRequest(txn_id_t txn_id, const RID &rid) {
  auto &queue = lock_table_[rid].request_queue_;
  for (auto it = queue.begin(); it != queue.end(); ++it) {
    if (it->txn_id_ == txn_id) {
      return it;
    }
  }
  return queue.end();
}
}  // namespace bustub
