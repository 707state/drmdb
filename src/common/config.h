#pragma once
#include "common/defs.h"
#include "parser/ast.h"
#include "record/rm_defs.h"
#include <atomic>
#include <chrono>
#define BUFFER_LENGTH 8192
#define OUTPUT_MODE
#ifdef OUTPUT_MODE
#define APPEND_TO_FILE(a, b, c)                                                                                        \
  {                                                                                                                    \
    std::string command = "echo " + std::to_string(a) + " " + std::to_string(b) + " " + c + " >> rid_count.txt";       \
    system(command.c_str());                                                                                           \
  }
#else
#define APPEND_TO_FILE(a, b, c)
#endif
#ifdef OUTPUT_MODE
#define APPEND_IID_TO_FILE(a, b, c)                                                                                    \
  {                                                                                                                    \
    std::string command = "echo " + std::to_string(a) + " " + std::to_string(b) + " " + c + " >> iid_count.txt";       \
    system(command.c_str());                                                                                           \
  }
#else
#define APPEND_IID_TO_FILE(a, b, c)
#endif
/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise. */
extern std::atomic<bool> enable_logging;

/** If ENABLE_LOGGING is true, the log should be flushed to disk every
 * LOG_TIMEOUT. */
extern std::chrono::duration<int64_t> log_timeout;

static constexpr int INVALID_FRAME_ID = -1;                // invalid frame id
static constexpr int INVALID_PAGE_ID = -1;                 // invalid page id
static constexpr int INVALID_TXN_ID = -1;                  // invalid transaction id
static constexpr int INVALID_TIMESTAMP = -1;               // invalid transaction timestamp
static constexpr int INVALID_LSN = -1;                     // invalid log sequence number
static constexpr int HEADER_PAGE_ID = 0;                   // the header page id
static constexpr int PAGE_SIZE = 4096;                     // size of a data page in byte  4KB
static constexpr int BUFFER_POOL_SIZE = 81920;             // size of buffer pool 256MB
static constexpr int LOG_BUFFER_SIZE = (1024 * PAGE_SIZE); // size of a log buffer in byte
static constexpr int BUCKET_SIZE = 50;                     // size of extendible hash bucket

using frame_id_t = int32_t; // frame id type, 帧页ID,
// 页在BufferPool中的存储单元称为帧,一帧对应一页
using page_id_t = int32_t;    // page id type , 页ID
using txn_id_t = int32_t;     // transaction id type
using lsn_t = int32_t;        // log sequence number type
using slot_offset_t = size_t; // slot offset type
using oid_t = uint16_t;
using timestamp_t = int32_t; // timestamp type, used for transaction concurrency

// log file
static const std::string LOG_FILE_NAME = "db.log";

// replacer
static const std::string REPLACER_TYPE = "LRU";

static const std::string DB_META_NAME = "db.meta";
struct TabCol {
  std::string tab_name;
  std::string col_name;
  ast::SvAggreType ag_type;
  std::string as_name;

  friend bool operator<(const TabCol &x, const TabCol &y) {
    return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
  }

  friend bool operator==(const TabCol &x, const TabCol &y) {
    return std::make_pair(x.tab_name, x.col_name) == std::make_pair(y.tab_name, y.col_name) && x.ag_type == y.ag_type;
  }
};

struct DateTime {
  int year, month, day, hour, minute, second;

  uint64_t encode() const {
    uint64_t ret = 0;
    ret |= static_cast<uint64_t>(year) << 40;
    ret |= static_cast<uint64_t>(month) << 32;
    ret |= static_cast<uint64_t>(day) << 24;
    ret |= static_cast<uint64_t>(hour) << 16;
    ret |= static_cast<uint64_t>(minute) << 8;
    ret |= static_cast<uint64_t>(second);
    return ret;
  }

  void decode(uint64_t code) {
    year = static_cast<int>((code >> 40) & 0xFFFF);
    month = static_cast<int>((code >> 32) & 0xFF);
    day = static_cast<int>((code >> 24) & 0xFF);
    hour = static_cast<int>((code >> 16) & 0xFF);
    minute = static_cast<int>((code >> 8) & 0xFF);
    second = static_cast<int>(code & 0xFF);
  }

  std::string encode_to_string() const {
    std::ostringstream oss;
    oss << year << "-";

    if (month < 10) {
      oss << "0";
    }
    oss << month << "-";

    if (day < 10) {
      oss << "0";
    }
    oss << day << " ";

    if (hour < 10) {
      oss << "0";
    }
    oss << hour << ":";

    if (minute < 10) {
      oss << "0";
    }
    oss << minute << ":";

    if (second < 10) {
      oss << "0";
    }
    oss << second;

    return oss.str();
  }

  void decode_from_string(const std::string &datetime_str) {
    std::istringstream iss(datetime_str);
    char delimiter;

    iss >> year >> delimiter >> month >> delimiter >> day >> hour >> delimiter >> minute >> delimiter >> second;
  }

  // 重载 > 运算符
  bool operator>(const DateTime &rhs) const { return encode() > rhs.encode(); }

  // 重载 < 运算符
  bool operator<(const DateTime &rhs) const { return encode() < rhs.encode(); }

  // 重载 == 运算符
  bool operator==(const DateTime &rhs) const { return encode() == rhs.encode(); }

  // 重载 >= 运算符
  bool operator>=(const DateTime &rhs) const { return encode() >= rhs.encode(); }

  // 重载 <= 运算符
  bool operator<=(const DateTime &rhs) const { return encode() <= rhs.encode(); }
};

struct Value {
  ColType type; // type of value
  union {
    int int_val;     // int value
    float float_val; // float value
  };
  std::string str_val; // string value

  DateTime datetime_val; // datetime value

  std::shared_ptr<RmRecord> raw; // raw record buffer

  void set_int(int int_val_) {
    type = TYPE_INT;
    int_val = int_val_;
  }

  void set_float(float float_val_) {
    type = TYPE_FLOAT;
    float_val = float_val_;
  }

  void set_str(std::string str_val_) {
    type = TYPE_STRING;
    str_val = std::move(str_val_);
  }

  void set_string(const std::string &str_val_) {
    type = TYPE_STRING;
    str_val = str_val_;
  }

  // 从uint64_t的形式转化为datetime
  void set_datetime(uint64_t datetime) {
    type = TYPE_DATETIME;
    datetime_val.decode(datetime);
    if (!check_datetime(datetime_val)) {
      throw TypeOverflowError("DateTime", datetime_val.encode_to_string());
    }
  }

  // 从'YYYY-MM-DD HH:MM:SS'的格式中提取对应信息，并检查是否合法
  void set_datetime(const std::string &datetime_str) {
    type = TYPE_DATETIME;
    datetime_val.decode_from_string(datetime_str);
    if (!check_datetime(datetime_val)) {
      throw TypeOverflowError("DateTime", datetime_str);
    }
  }

  void set_datetime(const DateTime &datetime) {
    type = TYPE_DATETIME;
    datetime_val = datetime;
    if (!check_datetime(datetime_val)) {
      throw TypeOverflowError("DateTime", datetime_val.encode_to_string());
    }
  }

  void init_raw(int len) {
    assert(raw == nullptr);
    raw = std::make_shared<RmRecord>(len);

    switch (type) {
    case TYPE_INT: {
      assert(len == sizeof(int));
      *(int *)(raw->data) = int_val;
      break;
    }
    case TYPE_FLOAT: {
      assert(len == sizeof(float));
      *(float *)(raw->data) = float_val;
      break;
    }
    case TYPE_STRING: {
      if (len < (int)str_val.size()) {
        throw StringOverflowError();
      }
      memcpy(raw->data, str_val.c_str(), str_val.size());
      // 直接填充剩余部分为0
      if (len > (int)str_val.size()) {
        memset(raw->data + str_val.size(), 0, len - str_val.size());
      }
      break;
    }
    case TYPE_DATETIME: {
      assert(len == sizeof(uint64_t));
      *(uint64_t *)(raw->data) = datetime_val.encode();
      break;
    }
    default: {
      throw InvalidTypeError();
    }
    }
  }

  // 检查datetime是否合法
  bool check_datetime(const DateTime &datetime) {
    // check year
    if (datetime.year < 1000 || datetime.year > 9999) {
      return false;
    }
    // check month
    if (datetime.month < 1 || datetime.month > 12) {
      return false;
    }

    int maxDay;
    switch (datetime.month) {
    case 1:
    case 3:
    case 5:
    case 7:
    case 8:
    case 10:
    case 12:
      maxDay = 31;
      break;
    case 4:
    case 6:
    case 9:
    case 11:
      maxDay = 30;
      break;
    case 2:
      // Check if it's a leap year
      if ((datetime.year % 4 == 0 && datetime.year % 100 != 0) || datetime.year % 400 == 0) {
        maxDay = 29;
      } else {
        maxDay = 28;
      }
      break;
    default:
      return false;
    }
    if (datetime.day < 1 || datetime.day > maxDay) {
      return false;
    }

    // check hour
    if (datetime.hour < 0 || datetime.hour > 23) {
      return false;
    }

    // check minute
    if (datetime.minute < 0 || datetime.minute > 59) {
      return false;
    }

    // check second
    if (datetime.second < 0 || datetime.second > 59) {
      return false;
    }

    return true;
  }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Condition {
  TabCol lhs_col;  // left-hand side column
  CompOp op;       // comparison operator
  bool is_rhs_val; // true if right-hand side is a value (not a column)
  TabCol rhs_col;  // right-hand side column
  Value rhs_val;   // right-hand side value
};

struct SetClause {
  TabCol lhs;
  Value rhs;
  bool is_plus_value = false;

  SetClause() {}

  SetClause(TabCol lhs_, Value rhs_) : lhs(std::move(lhs_)), rhs(std::move(rhs_)) {}

  SetClause(TabCol lhs_, Value rhs_, bool is_plus_value_)
      : lhs(std::move(lhs_)), rhs(std::move(rhs_)), is_plus_value(is_plus_value_) {}
};

// 增加聚合操作类型
enum AggreOp { AG_COUNT, AG_SUM, AG_MAX, AG_MIN };

// 增加OrderByCol类型
struct OrderByCol {
  TabCol tabcol;
  bool is_desc;
};
