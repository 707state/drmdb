#pragma once
#include "raft/basic_types.h"
#include "raft/log_entry.h"
#include "raft/msg_type.h"
#include "raft/ptr.h"
#include <raft/msg_base.h>
#include <raft/utils.h>
#include <sys/types.h>
#include <vector>
namespace raft {
// 请求消息
class req_msg : public msg_base {
public:
  req_msg(ulong term, msg_type type, int32 src, int32 dst, ulong last_log_term, ulong last_log_index, ulong commit_idx)
      : msg_base(term, type, src, dst), last_log_term_(last_log_term), last_log_idx_(last_log_index),
        commit_idx_(commit_idx) {}
  virtual ~req_msg() __override__;
  __nocopy__(req_msg);

public:
  ulong get_last_log_idx() const { return last_log_idx_; }
  ulong get_last_log_term() const { return last_log_term_; }
  ulong get_commit_idx() const { return commit_idx_; }
  std::vector<ptr<log_entry>> &log_entries() { return log_entries_; }

private:
  // 上一条日志的任期编号
  ulong last_log_term_;
  // 上一条日志的索引：目标节点当前的，如果下面的log_entries不为空，那么起始索引就是last_log_idx_+1
  ulong last_log_idx_;
  // 上一条提交日志的索引
  ulong commit_idx_;
  std::vector<ptr<log_entry>> log_entries_;
};
} // namespace raft
