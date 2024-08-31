#pragma once
#include "basic_types.h"
#include <algorithm>
namespace raft {
struct raft_params {
  enum return_method_type {
    blocking = 0x0,
    async_handler = 0x1,
  };
  enum locking_method_type {
    single_mutex = 0x0,
    dual_mutex = 0x1,
    dual_rw_lock = 0x2,
  };
  raft_params()
      : election_timeout_lower_bound_(250), election_timeout_upper_bound_(500), heartbeat_interval_(125),
        rpc_failure_backoff_(50), log_sync_batch_size_(1000), log_sync_stop_gap_(99999), snap_shot_block_size_(0),
        snapshot_distance_(0), enable_randomized_snap_creatioin_(false), max_append_size_(100),
        reserved_log_items_(100000), client_req_timeout_(3000), fresh_log_gap_(200), stale_log_gap_(2000),
        custom_commit_quorum_size_(0), custom_election_quorum_size_(0), leadership_expiry_(0),
        leadership_transfer_min_wait_time_(0), allow_temporary_zero_priority_leader_(true), auto_forwarding_(false),
        auto_forwarding_max_connections_(10), use_bg_thread_for_snapshot_io_(false),
        use_bg_thread_for_urgent_commit_(true), exclude_snp_receiver_from_quorom_(false),
        auto_adjust_quorum_for_smaller_cluster_(false), locking_method_type_(dual_mutex), return_method_type_(blocking),
        auto_forwarding_req_timeout_(0), grace_period_of_lagging_state_machine_(0),
        use_full_comsensus_among_healthy_members_(false), parrallel_log_appending_(false) {}
  raft_params &with_election_timeout_upper(int32 timeout) {
    election_timeout_upper_bound_ = timeout;
    return *this;
  }
  raft_params &with_election_timeout_lower(int32 timeout) {
    election_timeout_lower_bound_ = timeout;
    return *this;
  }
  raft_params &with_hb_interval(int32 hb_interval) {
    heartbeat_interval_ = hb_interval;
    return *this;
  }
  raft_params &with_rpc_failure_backoff(int32 backoff) {
    rpc_failure_backoff_ = backoff;
    return *this;
  }
  raft_params &with_max_append_size(int32 size) {
    max_append_size_ = size;
    return *this;
  }
  raft_params &with_log_sync_batch_size(int32 batch_size) {
    log_sync_batch_size_ = batch_size;
    return *this;
  }
  raft_params &with_log_sync_stopping_gap(int32 gap) {
    log_sync_stop_gap_ = gap;
    return *this;
  }
  raft_params &with_snapshot_enabled(int32 commit_distance) {
    snapshot_distance_ = commit_distance;
    return *this;
  }
  raft_params &with_randomized_snapshot_creation_enabled(bool enabled) {
    enable_randomized_snap_creatioin_ = enabled;
    return *this;
  }
  raft_params &with_snapshot_sync_block_size(int32 size) {
    snap_shot_block_size_ = size;
    return *this;
  }
  raft_params &with_reserved_log_items(int num_of_logs) {
    reserved_log_items_ = num_of_logs;
    return *this;
  }
  raft_params &with_client_req_timeout(int timeout) {
    client_req_timeout_ = timeout;
    return *this;
  }
  raft_params &with_auto_forwarding(bool enable) {
    auto_forwarding_ = enable;
    return *this;
  }
  raft_params &with_fresh_log_gap(int32 new_gap) {
    fresh_log_gap_ = new_gap;
    return *this;
  }
  raft_params &with_stale_log_gap(int32 new_gap) {
    stale_log_gap_ = new_gap;
    return *this;
  }
  raft_params &with_custom_commit_quorum_size(int32 new_size) {
    custom_commit_quorum_size_ = new_size;
    return *this;
  }
  raft_params &with_custom_election_quorum_size(int32 new_size) {
    custom_election_quorum_size_ = new_size;
    return *this;
  }
  raft_params &with_leadership_expiry(int32 expiry_ms) {
    leadership_expiry_ = expiry_ms;
    return *this;
  }
  raft_params &with_auto_forwarding_req_timeout(int32 timeout_ms) {
    auto_forwarding_req_timeout_ = timeout_ms;
    return *this;
  }
  int max_heartbeat_interval() const {
    return std::max(heartbeat_interval_, election_timeout_lower_bound_ - (heartbeat_interval_ / 2));
  }
  int32 election_timeout_upper_bound_;
  int32 election_timeout_lower_bound_;
  int32 heartbeat_interval_;
  int32 rpc_failure_backoff_;
  int32 log_sync_batch_size_;
  int32 log_sync_stop_gap_;
  int32 snapshot_distance_;
  int32 snap_shot_block_size_;
  bool enable_randomized_snap_creatioin_;
  int32 max_append_size_;
  int32 reserved_log_items_;
  int32 client_req_timeout_;
  int32 fresh_log_gap_;
  int32 stale_log_gap_;
  int32 custom_commit_quorum_size_;
  int32 custom_election_quorum_size_;
  int32 leadership_expiry_;
  int32 leadership_transfer_min_wait_time_;
  bool allow_temporary_zero_priority_leader_;
  bool auto_forwarding_;
  int32 auto_forwarding_max_connections_;
  bool use_bg_thread_for_urgent_commit_;
  bool exclude_snp_receiver_from_quorom_;
  bool auto_adjust_quorum_for_smaller_cluster_;
  locking_method_type locking_method_type_;
  return_method_type return_method_type_;
  int32 auto_forwarding_req_timeout_;
  int32 grace_period_of_lagging_state_machine_;
  bool use_bg_thread_for_snapshot_io_;
  bool use_full_comsensus_among_healthy_members_;
  bool parrallel_log_appending_;
};
} // namespace raft
