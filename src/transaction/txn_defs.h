#pragma once
#include "common/defs.h"
#include "record/rm_defs.h"
#include <boost/container_hash/hash.hpp>
#include <common/config.h>
#include <cstddef>
#include <cstdint>
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE };

class TableWriteRecord {
public:
  TableWriteRecord() = default;

  // constructor for insert operation
  TableWriteRecord(WType wtype, const std::string &tab_name, const Rid &rid)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

  // constructor for delete & update operation
  TableWriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const RmRecord &record)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record) {}

  ~TableWriteRecord() = default;

  inline RmRecord &GetRecord() { return record_; }

  inline Rid &GetRid() { return rid_; }

  inline WType &GetWriteType() { return wtype_; }

  inline std::string &GetTableName() { return tab_name_; }

private:
  WType wtype_;
  std::string tab_name_;
  Rid rid_;
  RmRecord record_;
};

class IndexWriteRecord {
public:
  IndexWriteRecord() = default;

  IndexWriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const char *key, int size)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid) {
    key_ = new char[size];
    memcpy(key_, key, size);
  }
  ~IndexWriteRecord() { delete key_; }

  //  ~IndexWriteRecord() = default;

  inline RmRecord &GetRecord() { return record_; }

  inline char *&GetKey() { return key_; }

  inline Rid &GetRid() { return rid_; }

  inline WType &GetWriteType() { return wtype_; }

  inline std::string &GetTableName() { return tab_name_; }

private:
  WType wtype_;
  std::string tab_name_;
  Rid rid_;
  char *key_;
  RmRecord record_;
};

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
public:
  /* 表级锁 */
  LockDataId(int fd, LockDataType type) {
    assert(type == LockDataType::TABLE);
    fd_ = fd;
    type_ = type;
    rid_.page_no = -1;
    rid_.slot_no = -1;
  }

  /* 行级锁 */
  LockDataId(int fd, const Rid &rid, LockDataType type) {
    assert(type == LockDataType::RECORD);
    fd_ = fd;
    rid_ = rid;
    type_ = type;
  }

  inline int64_t Get() const {
    if (type_ == LockDataType::TABLE) {
      // fd_
      return static_cast<int64_t>(fd_);
    } else {
      // fd_, rid_.page_no, rid.slot_no
      return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
             ((static_cast<int64_t>(rid_.page_no)) << 16) | rid_.slot_no;
    }
  }

  bool operator==(const LockDataId &other) const {
    if (type_ != other.type_) {
      return false;
    }
    if (fd_ != other.fd_) {
      return false;
    }
    return rid_ == other.rid_;
  }
  friend size_t hash_value(const LockDataId &other) { return other.Get(); }
  int fd_;
  Rid rid_;
  LockDataType type_;
};

template <> struct std::hash<LockDataId> {
  size_t operator()(const LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};
template <> struct boost::hash<LockDataId> {
  size_t operator()(const LockDataId &obj) const { return boost::hash<int64_t>()(obj.Get()); }
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHIRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION, WAIT_DIE_ABORT };

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
  txn_id_t txn_id_;
  AbortReason abort_reason_;

public:
  explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
      : txn_id_(txn_id), abort_reason_(abort_reason) {}

  txn_id_t get_transaction_id() { return txn_id_; }
  AbortReason GetAbortReason() { return abort_reason_; }
  std::string GetInfo() {
    switch (abort_reason_) {
    case AbortReason::LOCK_ON_SHIRINKING: {
      return "Transaction " + std::to_string(txn_id_) + " aborted because it cannot request locks on SHRINKING phase\n";
    } break;

    case AbortReason::UPGRADE_CONFLICT: {
      return "Transaction " + std::to_string(txn_id_) +
             " aborted because another transaction is waiting for upgrading\n";
    } break;

    case AbortReason::DEADLOCK_PREVENTION: {
      return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
    } break;

    default: {
      return "Transaction aborted";
    } break;
    }
  }
};
