#pragma once
#include "common/config.h"
#include "transaction/txn_defs.h"
#include <boost/unordered/unordered_set_fwd.hpp>
#include <boost/unordered_set.hpp>
#include <deque>
#include <memory>
#include <storage/page.h>
#include <thread>
class Transaction {
public:
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
      : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
    table_write_set_ = std::make_shared<std::deque<std::shared_ptr<TableWriteRecord>>>();
    index_write_set_ = std::make_shared<std::deque<std::shared_ptr<IndexWriteRecord>>>();
    lock_set_ = std::make_shared<boost::unordered_set<LockDataId>>();
    index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
    index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
    prev_lsn_ = INVALID_LSN;
    thread_id_ = std::this_thread::get_id();
  }
  ~Transaction() = default;
  inline txn_id_t get_transaction_id() { return txn_id_; }
  inline std::thread::id get_thread_id() { return thread_id_; }
  inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
  inline bool get_txn_mode() { return txn_mode_; }
  inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
  inline timestamp_t get_start_ts() { return start_ts_; }
  inline IsolationLevel get_isolation_level() { return isolation_level_; }
  inline TransactionState get_state() { return state_; }
  inline void set_state(TransactionState state) { state_ = state; }
  inline lsn_t get_prev_lsn() { return prev_lsn_; }
  inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }
  inline std::shared_ptr<std::deque<std::shared_ptr<TableWriteRecord>>> get_table_write_set() {
    return table_write_set_;
  }
  inline void append_table_write_record(std::shared_ptr<TableWriteRecord> write_record) {
    table_write_set_->push_back(write_record);
  }
  inline std::shared_ptr<std::deque<std::shared_ptr<IndexWriteRecord>>> get_index_write_set() {
    return index_write_set_;
  }
  inline void append_index_write_set(std::shared_ptr<IndexWriteRecord> index_write) {
    index_write_set_->push_back(index_write);
  }
  inline void append_index_write_record(std::shared_ptr<IndexWriteRecord> write_record) {
    index_write_set_->push_back(write_record);
  }
  inline std::shared_ptr<std::deque<Page *>> get_index_deleted_page_set() { return index_deleted_page_set_; }
  inline std::shared_ptr<std::deque<Page *>> get_index_latch_page_set() { return index_latch_page_set_; }
  inline void append_index_deleted_page(Page *page) { this->index_deleted_page_set_->push_back(page); }
  inline void append_index_latch_page_set(Page *page) { index_latch_page_set_->push_back(page); }
  inline std::shared_ptr<boost::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

private:
  bool txn_mode_;
  TransactionState state_;
  IsolationLevel isolation_level_;
  std::thread::id thread_id_;
  lsn_t prev_lsn_;
  txn_id_t txn_id_;
  timestamp_t start_ts_;
  std::shared_ptr<std::deque<std::shared_ptr<TableWriteRecord>>> table_write_set_;
  std::shared_ptr<std::deque<std::shared_ptr<IndexWriteRecord>>> index_write_set_;
  std::shared_ptr<boost::unordered_set<LockDataId>> lock_set_;
  std::shared_ptr<std::deque<Page *>> index_latch_page_set_;
  std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;
};
