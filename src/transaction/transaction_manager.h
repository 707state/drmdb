#pragma once
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include <boost/unordered_map.hpp>
#include <utility>
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO };

class TransactionManager {
public:
  explicit TransactionManager(std::shared_ptr<LockManager> lock_manager, std::shared_ptr<SmManager> sm_manager,
                              ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING)
      : concurrency_mode_(concurrency_mode), sm_manager_(std::move(sm_manager)),
        lock_manager_(std::move(lock_manager)) {}

  ~TransactionManager() = default;
  Transaction *begin(Transaction *txn, std::shared_ptr<LogManager> log_manager);

  std::shared_ptr<Transaction> begin();
  void commit(std::shared_ptr<Transaction> txn, std::shared_ptr<LogManager> log_manager);

  void abort(std::shared_ptr<Transaction> txn, std::shared_ptr<LogManager> log_manager);

  ConcurrencyMode get_concurrency_mode() { return concurrency_mode_; }

  void set_concurrency_mode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

  std::shared_ptr<LockManager> get_lock_manager() { return lock_manager_; }

  /**
   * @description: 获取事务ID为txn_id的事务对象
   * @return {Transaction*} 事务对象的指针
   * @param {txn_id_t} txn_id 事务ID
   */
  std::shared_ptr<Transaction> get_transaction(txn_id_t txn_id) {
    if (txn_id == INVALID_TXN_ID) {
      return nullptr;
    }

    std::unique_lock<std::mutex> lock(latch_);
    assert(TransactionManager::txn_map.find(txn_id) != TransactionManager::txn_map.end());
    auto res = TransactionManager::txn_map[txn_id];
    lock.unlock();
    assert(res != nullptr);
    assert(res->get_thread_id() == std::this_thread::get_id());

    return res;
  }

  static boost::unordered_map<txn_id_t, std::shared_ptr<Transaction>>
      txn_map; // 全局事务表，存放事务ID与事务对象的映射关系

private:
  ConcurrencyMode concurrency_mode_;           // 事务使用的并发控制算法，目前只需要考虑2PL
  std::atomic<txn_id_t> next_txn_id_{0};       // 用于分发事务ID
  std::atomic<timestamp_t> next_timestamp_{0}; // 用于分发事务时间戳
  std::mutex latch_;                           // 用于txn_map的并发
  std::shared_ptr<SmManager> sm_manager_;
  std::shared_ptr<LockManager> lock_manager_;
};
