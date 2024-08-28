#pragma once
#include "fmt/core.h"
#include "fmt/format.h"
#include <boost/lexical_cast.hpp>
#include <boost/operators.hpp>
#include <cerrno>
#include <cstring>
#include <exception>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <vector>
class DRMDBError : public std::exception {
public:
  DRMDBError() : _msg("Error: ") {}
  explicit DRMDBError(const std::string &msg) : _msg("Error: " + msg) { spdlog::error(_msg); }
  const char *what() const noexcept override { return _msg.c_str(); }
  auto get_msg_len() -> int { return _msg.length(); }
  std::string_view get_msg() { return _msg; }
  std::string _msg;
};

class InternalError : public DRMDBError {
public:
  explicit InternalError(const std::string &msg) : DRMDBError(msg) {}
};

class UnixError : public DRMDBError {
public:
  UnixError() : DRMDBError(strerror(errno)) {}
};

class FileNotOpenError : public DRMDBError {
public:
  explicit FileNotOpenError(int fd) : DRMDBError("Invalid file descriptor: " + boost::lexical_cast<std::string>(fd)) {}
};

class FileNotClosedError : public DRMDBError {
public:
  explicit FileNotClosedError(const std::string &file_name) : DRMDBError("File is opened: " + file_name) {}
};

class FileExistsError : public DRMDBError {
public:
  explicit FileExistsError(const std::string &file_name) : DRMDBError("File already exists: " + file_name) {}
};
class FileNotFoundError : public DRMDBError {
public:
  explicit FileNotFoundError(const std::string &file_name) : DRMDBError("File not found: " + file_name) {}
};

// Record
class RecordNotFoundError : public DRMDBError {
public:
  RecordNotFoundError(int page_no, int slot_no)
      : DRMDBError(fmt::format("Record not foun: ({}, {})", page_no, slot_no)) {}
};

class InvalidRecordSizeError : public DRMDBError {
public:
  explicit InvalidRecordSizeError(int record_size)
      : DRMDBError("Invalid record size: " + std::to_string(record_size)) {}
};

class InvalidSlotNoError : public DRMDBError {
public:
  InvalidSlotNoError(int slot_no, int record_num)
      : DRMDBError(fmt::format("Invalid slot no: {}, Num reocrd per page: {}", slot_no, record_num)) {}
};

// IX
class InvalidColLengthError : public DRMDBError {
public:
  explicit InvalidColLengthError(int col_len) : DRMDBError(fmt::format("Invalid column length: {}", col_len)) {}
};

class IndexEntryNotFoundError : public DRMDBError {
public:
  IndexEntryNotFoundError() : DRMDBError("Index entry not found") {}
};
// System
class DatabaseNotFoundError : public DRMDBError {
public:
  explicit DatabaseNotFoundError(const std::string &db_name) : DRMDBError("Database not found" + db_name) {}
};
class DatabaseExistsError : public DRMDBError {
public:
  explicit DatabaseExistsError(const std::string &db_name) : DRMDBError("Database already exists: " + db_name) {}
};
class TableNotFound : public DRMDBError {
public:
  explicit TableNotFound(const std::string &table_name) : DRMDBError("Table not found: " + table_name) {}
};
class TableExisisError : public DRMDBError {
public:
  explicit TableExisisError(const std::string &tab_name) : DRMDBError("Table already exists: " + tab_name) {}
};
class ColumnNotFoundError : public DRMDBError {
public:
  explicit ColumnNotFoundError(const std::string &col_name) : DRMDBError("Column not found: " + col_name) {}
};

class IndexNotFoundError : public DRMDBError {
public:
  IndexNotFoundError(const std::string &tab_name, const std::vector<std::string> &col_names) {
    _msg = fmt::format("Index not found: {}.({})", tab_name, fmt::join(col_names, ", "));
  }
};

class IndexExistsError : public DRMDBError {
public:
  IndexExistsError(const std::string &tab_name, const std::vector<std::string> &col_names) {
    _msg = fmt::format("Index already exists: {}.({})", tab_name, fmt::join(col_names, ", "));
  }
};
class InvalidValueCountError : public DRMDBError {
public:
  InvalidValueCountError() : DRMDBError("Invalid value count") {}
};
class StringOverflowError : public DRMDBError {
public:
  StringOverflowError() : DRMDBError("String is too long") {}
};
class IncompatibleTypeError : public DRMDBError {
public:
  IncompatibleTypeError(const std::string &lhs, const std::string &rhs)
      : DRMDBError(fmt::format("Incompatible type error: lhs: {} , rhs: {}", lhs, rhs)) {}
};
class AmbiguousColumnError : public DRMDBError {
public:
  explicit AmbiguousColumnError(const std::string &col_name) : DRMDBError("Ambiguous column: " + col_name) {}
};
class PageNotExistsError : public DRMDBError {
public:
  PageNotExistsError(const std::string &table_name, int page_no)
      : DRMDBError(fmt::format("Page {} in table {} not exists", page_no, table_name)) {}
};

class InvalidTypeError : public DRMDBError {
public:
  InvalidTypeError() : DRMDBError("Invalid Type Error: Cannot convert to record") {}
};
class TypeOverflowError : public DRMDBError {
public:
  TypeOverflowError(const std::string &type, const std::string &bigint_val)
      : DRMDBError(fmt::format("{} OVERFLOW: {}", type, bigint_val)) {}
};
