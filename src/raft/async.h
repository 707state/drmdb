#pragma once
#include "ptr.h"
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
namespace raft {
enum cmd_result_code {
  OK = 0,
  CANCELED = -1,
  TIMEOUT = -2,
  NOT_LEADER = -3,
  BAD_REQUEST = -4,
  SERVER_ALREADY_EXISTS = -5,
  CONFIG_CHANGING = -6,
  SERVER_IS_JOINING = -7,
  SERVER_NOT_FOUND = -8,
  CANNOT_REMOVE_LEADER = -9,
  SERVER_IS_LEAVING = -10,
  TERM_MISMATCH = -11,
  RESULT_NOT_EXIST_YET = -1000,
  FAILED = -32768,
};
template <typename T, typename TE = ptr<std::exception>> class cmd_result {
  // 只会被结果触发
  using handler_type = std::function<void(T &, TE &)>;
  // 会被当前实例触发
  using handler_type2 = std::function<void(cmd_result<T, TE> &, TE &)>;

private:
  T empty_result;
  T result_;
  TE err_;
  cmd_result_code code_;
  bool has_result_;
  bool accepted_;
  handler_type handler_;
  handler_type2 handler2_;
  mutable std::mutex lock_;
  std::condition_variable cv_;
};
template <typename T, typename TE = ptr<std::mutex>> using async_result = cmd_result<T, TE>;
} // namespace raft
