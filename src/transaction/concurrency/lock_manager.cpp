#include "transaction/concurrency/lock_manager.h"
#include "common/config.h"
#include "common/defs.h"
#include "transaction/transaction.h"
#include "transaction/txn_defs.h"
#include <mutex>
#include <tuple>
#include <utility>
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
  std::unique_lock<std::mutex> lock{latch_};
  while (true) {
    if (check_lock(txn)) {
      return false;
    }
    LockDataId lock_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    LockRequest request(txn->get_transaction_id(), LockMode::SHARED, txn->get_start_ts());
    LockRequestQueue &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        return true;
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.lock_mode_ != LockMode::SHARED) {
        timestamp_t current_txn_ts = txn->get_start_ts();
        timestamp_t existing_txn_ts = it.txn_ts_;
        // 申请🔒的事务的时间戳大于持有锁的
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() {
        return request_q.group_lock_mode_ == GroupLockMode::NON_LOCK || request_q.group_lock_mode_ == GroupLockMode::S;
      });
      continue;
    }
    if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK || request_q.group_lock_mode_ == GroupLockMode::S) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      request.granted_ = true;
      request_q.group_lock_mode_ = GroupLockMode::S;
      request_q.request_queue_.emplace_back(request);
    } else {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
  std::unique_lock<std::mutex> lock{latch_};
  while (true) {
    if (!check_lock(txn)) {
      return false;
    }
    LockDataId lock_id = LockDataId{tab_fd, rid, LockDataType::RECORD};
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    LockRequest request(txn->get_transaction_id(), LockMode::EXCLUSIVE, txn->get_start_ts());
    auto &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        if (it.lock_mode_ == LockMode::EXCLUSIVE) {
          return true;
        } else {
          if (request_q.group_lock_mode_ == GroupLockMode::S && request_q.request_queue_.size() == 1) {
            it.lock_mode_ = LockMode::EXCLUSIVE;
            request_q.group_lock_mode_ = GroupLockMode::X;
            return true;
          } else {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
          }
        }
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.granted_) {
        auto current_txn_ts = txn->get_start_ts();
        auto existing_txn_ts = it.txn_ts_;
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() { return request_q.group_lock_mode_ == GroupLockMode::NON_LOCK; });
      continue;
    }
    if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      request.granted_ = true;
      request_q.group_lock_mode_ = GroupLockMode::X;
      request_q.request_queue_.emplace_back(request);
    } else {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd) {
  std::unique_lock<std::mutex> lock{latch_};
  while (true) {
    if (!check_lock(txn)) {
      return false;
    }
    auto lock_id = LockDataId{tab_fd, LockDataType::TABLE};
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    LockRequest requst{txn->get_transaction_id(), LockMode::SHARED, txn->get_start_ts()};
    auto &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        if (it.lock_mode_ == LockMode::EXCLUSIVE || it.lock_mode_ == LockMode::S_IX ||
            it.lock_mode_ == LockMode::SHARED) {
          return true;
        } else if (it.lock_mode_ == LockMode::INTENTION_SHARED) {
          if (request_q.group_lock_mode_ == GroupLockMode::IS || request_q.group_lock_mode_ == GroupLockMode::S) {
            it.lock_mode_ = LockMode::SHARED;
            request_q.group_lock_mode_ = GroupLockMode::S;
            return true;
          } else {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
          }
        } else {
          int num = std::count_if(request_q.request_queue_.begin(), request_q.request_queue_.end(),
                                  [](const auto &it2) { return it2.lock_mode_ == LockMode::INTENTION_EXCLUSIVE; });
          if (num == 1) {
            it.lock_mode_ = LockMode::S_IX;
            request_q.group_lock_mode_ = GroupLockMode::SIX;
            return true;
          }
        }
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && (it.lock_mode_ == LockMode::EXCLUSIVE || it.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                          it.lock_mode_ == LockMode::S_IX)) {
        auto current_txn_ts = txn->get_start_ts();
        auto existing_txn_ts = it.txn_ts_;
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else [[likely]] {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() {
        return request_q.group_lock_mode_ == GroupLockMode::NON_LOCK ||
               request_q.group_lock_mode_ == GroupLockMode::S || request_q.group_lock_mode_ == GroupLockMode::IS;
      });
      continue;
    }
    if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK || request_q.group_lock_mode_ == GroupLockMode::S ||
        request_q.group_lock_mode_ == GroupLockMode::IS) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      requst.granted_ = true;
      request_q.group_lock_mode_ = GroupLockMode::S;
      request_q.request_queue_.emplace_back(requst);
      return true;
    } else [[unlikely]] {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}

bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd) {
  std::unique_lock<std::mutex> lock{latch_};
  while (true) {
    if (!check_lock(txn)) {
      return false;
    }
    auto lock_id = LockDataId{tab_fd, LockDataType::TABLE};
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    auto request = LockRequest{txn->get_transaction_id(), LockMode::EXCLUSIVE, txn->get_start_ts()};
    auto &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        if (it.lock_mode_ == LockMode::EXCLUSIVE) {
          return true;
        } else {
          if (request_q.request_queue_.size() == 1) {
            it.lock_mode_ = LockMode::EXCLUSIVE;
            request_q.group_lock_mode_ = GroupLockMode::X;
            return true;
          } else [[unlikely]] {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
          }
        }
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.granted_) {
        auto current_txn_ts = txn->get_start_ts();
        auto existing_txn_ts = it.txn_ts_;
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else [[likely]] {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() { return request_q.group_lock_mode_ == GroupLockMode::NON_LOCK; });
      continue;
    }
    if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      request.granted_ = true;
      request_q.group_lock_mode_ = GroupLockMode::X;
      request_q.request_queue_.emplace_back(request);
      return true;
    } else [[unlikely]] {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}

bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd) {
  std::unique_lock lock{latch_};
  while (true) {
    if (!check_lock(txn)) {
      return false;
    }
    auto lock_id = LockDataId{tab_fd, LockDataType::TABLE};
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    auto request = LockRequest{txn->get_transaction_id(), LockMode::INTENTION_SHARED, txn->get_start_ts()};
    auto &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        return true;
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.lock_mode_ == LockMode::EXCLUSIVE) {
        auto current_txn_ts = txn->get_start_ts();
        auto existing_txn_ts = it.txn_ts_;
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else [[likely]] {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() { return request_q.group_lock_mode_ != GroupLockMode::X; });
      continue;
    }
    if (request_q.group_lock_mode_ != GroupLockMode::X) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      request.granted_ = true;
      if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        request_q.group_lock_mode_ = GroupLockMode::IS;
      }
      request_q.request_queue_.emplace_back(request);
      return true;
    } else [[unlikely]] {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd) {
  std::unique_lock<std::mutex> lock{latch_};
  while (true) {
    if (!check_lock(txn)) {
      return false;
    }
    auto lock_id = LockDataId{tab_fd, LockDataType::TABLE};
    if (!lock_table_.count(lock_id)) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_id), std::forward_as_tuple());
    }
    auto request = LockRequest{txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE, txn->get_start_ts()};
    auto &request_q = lock_table_[lock_id];
    for (auto &it : request_q.request_queue_) {
      if (it.granted_ && it.txn_id_ == txn->get_transaction_id()) {
        if (it.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || it.lock_mode_ == LockMode::S_IX ||
            it.lock_mode_ == LockMode::EXCLUSIVE) {
          return true;
        } else if (it.lock_mode_ == LockMode::SHARED) {
          int num = std::count_if(request_q.request_queue_.begin(), request_q.request_queue_.end(),
                                  [](const auto &iter) { return iter.lock_mode_ == LockMode::SHARED; });
          if (num == 1) {
            it.lock_mode_ = LockMode::S_IX;
            request_q.group_lock_mode_ = GroupLockMode::SIX;
            return true;
          }
        } else {
          if (request_q.group_lock_mode_ == GroupLockMode::IS || request_q.group_lock_mode_ == GroupLockMode::IX) {
            it.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
            request_q.group_lock_mode_ = GroupLockMode::IX;
            return true;
          } else [[unlikely]] {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
          }
        }
      }
    }
    bool wait_required = false;
    for (auto &it : request_q.request_queue_) {
      if (it.txn_id_ != txn->get_transaction_id() && it.granted_ &&
          (it.lock_mode_ == LockMode::SHARED || it.lock_mode_ == LockMode::EXCLUSIVE ||
           it.lock_mode_ == LockMode::S_IX)) {
        auto current_txn_ts = txn->get_start_ts();
        auto existing_txn_ts = it.txn_ts_;
        if (current_txn_ts < existing_txn_ts) {
          wait_required = true;
          break;
        } else [[likely]] {
          throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WAIT_DIE_ABORT);
        }
      }
    }
    if (wait_required) {
      request_q.cv_.wait(lock, [&]() {
        return std::none_of(request_q.request_queue_.begin(), request_q.request_queue_.end(),
                            [&](const LockRequest &lr) {
                              return lr.txn_id_ != txn->get_transaction_id() && lr.granted_ &&
                                     (lr.lock_mode_ == LockMode::SHARED || lr.lock_mode_ == LockMode::EXCLUSIVE ||
                                      lr.lock_mode_ == LockMode::S_IX);
                            });
      });
      continue;
    }
    if (request_q.group_lock_mode_ == GroupLockMode::NON_LOCK || request_q.group_lock_mode_ == GroupLockMode::IS ||
        request_q.group_lock_mode_ == GroupLockMode::IX) {
      auto lock_set = txn->get_lock_set();
      lock_set->emplace(lock_id);
      request.granted_ = true;
      request_q.group_lock_mode_ = GroupLockMode::IX;
      request_q.request_queue_.emplace_back(request);
      return true;
    } else [[unlikely]] {
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
  }
}

bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id) {
  std::scoped_lock<std::mutex> lock{latch_};
  if (!check_unlock(txn)) {
    return false;
  }
  if (!lock_table_.count(lock_data_id)) {
    return true;
  }
  auto &request_q = lock_table_[lock_data_id];
  auto it = std::remove_if(request_q.request_queue_.begin(), request_q.request_queue_.end(),
                           [&txn](const LockRequest &request) { return request.txn_id_ == txn->get_transaction_id(); });
  if (it != request_q.request_queue_.end()) {
    request_q.request_queue_.erase(it, request_q.request_queue_.end());
  }
  int IS_num = 0, IX_num = 0, S_num = 0, SIX_num = 0, X_num = 0;
  for (const auto &it : request_q.request_queue_) {
    switch (it.lock_mode_) {
    case LockMode::INTENTION_SHARED:
      IS_num++;
    case LockMode::INTENTION_EXCLUSIVE:
      IX_num++;
    case LockMode::SHARED:
      S_num++;
    case LockMode::EXCLUSIVE:
      X_num++;
    case LockMode::S_IX:
      SIX_num++;
    }
  }
  if (X_num) {
    request_q.group_lock_mode_ = GroupLockMode::X;
  } else if (SIX_num) {
    request_q.group_lock_mode_ = GroupLockMode::SIX;
  } else if (IX_num) {
    request_q.group_lock_mode_ = GroupLockMode::IX;
  } else if (S_num) {
    request_q.group_lock_mode_ = GroupLockMode::S;
  } else if (IS_num) {
    request_q.group_lock_mode_ = GroupLockMode::IS;
  } else {
    request_q.group_lock_mode_ = GroupLockMode::NON_LOCK;
  }
  request_q.cv_.notify_all();
  return true;
}
bool LockManager::check_lock(Transaction *txn) {
  if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
    return false;
  } else if (txn->get_state() == TransactionState::DEFAULT) {
    txn->set_state(TransactionState::GROWING);
    return true;
  } else if (txn->get_state() == TransactionState::GROWING) {
    return true;
  } else if (txn->get_state() == TransactionState::SHRINKING) {
    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
  } else [[unlikely]] {
    throw DRMDBError("事务状态无效：" + std::string(__FILE__) + ":" + std::to_string(__LINE__));
  }
  return false;
}

bool LockManager::check_unlock(Transaction *txn) {
  if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
    return false;
  } else if (txn->get_state() == TransactionState::DEFAULT) {
    return true;
  } else if (txn->get_state() == TransactionState::GROWING) {
    txn->set_state(TransactionState::SHRINKING);
    return true;
  } else if (txn->get_state() == TransactionState::SHRINKING) {
    return true;
  } else [[unlikely]] {
    throw DRMDBError("事务状态无效: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
  }
  return false;
}
