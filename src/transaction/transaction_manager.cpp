#include "common/config.h"
#include "transaction/transaction.h"
#include <boost/unordered_map.hpp>
#include <transaction/transaction_manager.h>
boost::unordered_map<txn_id_t, std::shared_ptr<Transaction>> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn
 * 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, std::shared_ptr<LogManager> log_manager) {
  // Todo:
  // 1. 判断传入事务参数是否为空指针
  // 2. 如果为空指针，创建新事务
  // 3. 把开始事务加入到全局事务表中
  // 4. 返回当前事务指针
  std::scoped_lock lock(latch_);

  if (txn == nullptr) {
    txn = new Transaction(next_txn_id_);
    next_txn_id_++;
    txn->set_start_ts(next_timestamp_++);
  }

  txn_map[txn->get_transaction_id()] = std::shared_ptr<Transaction>(txn);
  txn->set_state(TransactionState::DEFAULT);

  return txn;
}
std::shared_ptr<Transaction> TransactionManager::begin() {
  // Todo:
  // 1. 判断传入事务参数是否为空指针
  // 2. 如果为空指针，创建新事务
  // 3. 把开始事务加入到全局事务表中
  // 4. 返回当前事务指针

  std::scoped_lock lock(latch_);
  auto txn = std::make_shared<Transaction>(next_txn_id_);
  next_txn_id_++;
  txn->set_start_ts(next_timestamp_++);
  txn_map[txn->get_transaction_id()] = txn;
  txn->set_state(TransactionState::DEFAULT);

  return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(std::shared_ptr<Transaction> txn, std::shared_ptr<LogManager> log_manager) {
  // Todo:
  // 1. 如果存在未提交的写操作，提交所有的写操作
  // 2. 释放所有锁
  // 3. 释放事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  std::scoped_lock lock(latch_);

  for (auto const &locked : *(txn->get_lock_set())) {
    lock_manager_->unlock(txn.get(), locked);
  }

  // 释放写集
  auto table_write_set = txn->get_table_write_set();
  table_write_set->clear();
  auto get_index_write_set = txn->get_index_write_set();
  get_index_write_set->clear();

  // 释放锁集
  txn->get_lock_set()->clear();

  txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(std::shared_ptr<Transaction> txn, std::shared_ptr<LogManager> log_manager) {
  // Todo:
  // 1. 回滚所有写操作
  // 2. 释放所有锁
  // 3. 清空事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  std::scoped_lock lock(latch_);

  Context context(lock_manager_, log_manager, txn);

  auto table_set = txn->get_table_write_set();

  while (!table_set->empty()) {
    auto write_record = table_set->back();
    auto &rm_file = sm_manager_->fhs_.at(write_record->GetTableName());

    if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
      rm_file->delete_record(write_record->GetRid(), &context);
    } else if (write_record->GetWriteType() == WType::DELETE_TUPLE) {
      rm_file->insert_record(write_record->GetRecord().data, &context);
    } else if (write_record->GetWriteType() == WType::UPDATE_TUPLE) {
      rm_file->update_record(write_record->GetRid(), write_record->GetRecord().data, &context);
    }

    table_set->pop_back();
  }
  table_set->clear();

  auto index_set = txn->get_index_write_set();
  while (!index_set->empty()) {
    auto index_record = index_set->back();
    auto indexes = sm_manager_->db_.get_table(index_record->GetTableName()).indexes;

    if (index_record->GetWriteType() == WType::INSERT_TUPLE) {
      for (auto &index : indexes) {
        auto ih = sm_manager_->ihs_
                      .at(sm_manager_->get_ix_manager()->get_index_name(index_record->GetTableName(), index.cols))
                      .get();
        ih->delete_entry(index_record->GetKey(), txn.get());
      }
    } else if (index_record->GetWriteType() == WType::DELETE_TUPLE) {
      for (auto &index : indexes) {
        auto ih = sm_manager_->ihs_
                      .at(sm_manager_->get_ix_manager()->get_index_name(index_record->GetTableName(), index.cols))
                      .get();
        ih->insert_entry(index_record->GetKey(), index_record->GetRid(), txn.get());
      }
    }
    index_set->pop_back();
  }

  index_set->clear();

  for (auto const &locked : *(txn->get_lock_set())) {
    lock_manager_->unlock(txn.get(), locked);
  }

  txn->get_lock_set()->clear();

  txn->set_state(TransactionState::ABORTED);
}
