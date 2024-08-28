#pragma once
#include <utility>

#include <utility>

#include "recovery/log_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction.h"
static int const_offset = -1;

class Context {
public:
  Context(std::shared_ptr<LockManager> lock_mgr, std::shared_ptr<LogManager> log_mgr, std::shared_ptr<Transaction> txn,
          char *data_send = nullptr, int *offset = &const_offset)
      : lock_mgr_(std::move(lock_mgr)), log_mgr_(std::move(log_mgr)), txn_(std::move(txn)), data_send_(data_send),
        offset_(offset), ellipsis_(false) {}

  // TransactionManager *txn_mgr_;
  std::shared_ptr<LockManager> lock_mgr_;
  std::shared_ptr<LogManager> log_mgr_;
  std::shared_ptr<Transaction> txn_;
  char *data_send_;
  int *offset_;
  bool ellipsis_;
};
