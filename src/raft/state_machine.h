#pragma once
#include "async.h"
#include "basic_types.h"
#include "buffer.h"
#include "ptr.h"
#include <boost/unordered/unordered_map_fwd.hpp>
#include <cstdint>
#include <raft/utils.h>
namespace raft {
class cluster_config;
class snapshot;
class state_machine {
  __interface_body(state_machine);

public:
  struct ext_op_params {
    ext_op_params(ulong log_idx, ptr<buffer> &data) : log_idx(log_idx), data(data) {}
    ulong log_idx;
    ptr<buffer> &data;
  };
  virtual ptr<buffer> commit(const ulong log_idx, buffer &data) { return nullptr; }
  virtual ptr<buffer> commit_ext(const ext_op_params &params) { return commit(params.log_idx, *params.data); }
  virtual void commit_config(const ulong log_idx, ptr<cluster_config> &new_conf) {}
  virtual ptr<buffer> pre_commit(const ulong log_idx, buffer &data) { return nullptr; }
  virtual ptr<buffer> pre_commit_ext(const ext_op_params &params) { return pre_commit(params.log_idx, *params.data); }
  virtual void rollback(const ulong log_idx, buffer &data) {}
  virtual void rollback_ext(const ext_op_params &params) { rollback(params.log_idx, *params.data); }
  virtual int64 get_next_batch_size_hint_in_bytes() { return 0; }
  virtual void save_snapshot_data(snapshot &s, const ulong offset, buffer &data) {}
  virtual void save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer &data, bool is_first_obj, bool is_last_obj) {}
  virtual bool apply_snapshot(snapshot &s) = 0;
  virtual int read_snapshot_dat(snapshot &s, const ulong offset, buffer &data) { return 0; }
  virtual void free_user_snp_ctx(void *&user_snp_ctx) {}
  virtual ptr<snapshot> last_snapshot() = 0;
  virtual ulong last_commit_index() = 0;
  virtual void create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) = 0;
  virtual bool chk_create_snapshot() { return true; }
  virtual bool allow_leadership_transfer() { return true; }
  struct adjust_commit_index_params {
    adjust_commit_index_params() : current_commit_index_(0), expected_commit_index_(0) {}
    uint64_t current_commit_index_;
    uint64_t expected_commit_index_;
    boost::unordered_map<int, uint64_t> peer_index_map_; // 存储每一个节点持有的最新日志索引
  };
  virtual uint64_t adjust_commit_index(const adjust_commit_index_params &params) {
    return params.expected_commit_index_;
  }

public:
};
} // namespace raft
