#pragma once
#include "basic_types.h"
#include "delayed_task.h"
#include "delayed_task_scheduler.h"
#include "internal_timer.h"
#include "ptr.h"
#include "raft_server.h"
#include "req_msg.h"
#include "rpc_cli.h"
#include "rpc_exception.h"
#include "srv_config.h"
#include "timer_task.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <raft/context.h>
#include <raft/snapshot_sync_ctx.h>
#include <sys/types.h>
#include <unistd.h>
namespace raft {
class snapshot;
class peer {
public:
  peer(ptr<srv_config> &config, const context &ctx, timer_task<int32>::executor &hb_exec, ptr<logger> &logger)
      : config_(config), scheduler_(ctx.scheduler_), rpc_(ctx.rpc_cli_factory_->create_client(config->get_endpoint())),
        current_heartbeat_interval_(ctx.get_params()->heartbeat_interval_),
        heartbeat_interval_(ctx.get_params()->heartbeat_interval_),
        rpc_backoff_(ctx.get_params()->rpc_failure_backoff_),
        max_heartbeat_interval_(ctx.get_params()->max_heartbeat_interval()), next_log_idx_(0),
        last_accepted_log_idx_(0), next_batch_size_hint_in_bytes_(0), matched_idx_(0), busy_flag_(false),
        pending_commit_flag_(false), heartbear_enabled_(false),
        heartbeat_task_(new_ptr<timer_task<int32>, timer_task<int32>::executor &, int32>(
            hb_exec, config->get_id(), timer_task_type::heartbeat_timer)),
        snp_sync_ctx_(nullptr), lock_(), long_pause_warnings_(0), network_recoveries_(0), manual_free_(false),
        rpc_errs_(0), last_sent_idx_(0), cnt_not_applied_(0), leave_requested_(false), heartbeat_cnt_since_leave_(0),
        stepping_down_(false), reconn_scheduled_(false), reconn_backoff_(0), suppress_following_error_(false),
        abandoned_(false), rsv_msg_(nullptr), rsv_msg_handler_(nullptr), logger_(logger) {
    reset_last_sent_timer();
    reset_resp_timer();
    reset_active_timer();
  }

  __nocopy__(peer);

public:
  int32 get_id() const { return config_->get_id(); }
  const std::string &get_endpoint() const { return config_->get_endpoint(); }
  bool is_learner() const { return config_->is_learner(); }
  const srv_config &get_config() { return *config_; }
  void set_config(ptr<srv_config> new_config) { config_ = new_config; }
  ptr<delayed_task> &get_heartbeat_task() { return heartbeat_task_; }
  std::mutex &get_lock() { return lock_; }
  int32 get_current_heartbeat_interval() const { return current_heartbeat_interval_; }
  bool make_busy() {
    bool f = false;
    return busy_flag_.compare_exchange_strong(f, true);
  }
  bool is_busy() { return busy_flag_; }
  void set_free() { busy_flag_.store(false); }
  bool is_heartbeat_enabled() const { return is_heartbeat_enabled(); }
  // 启动心跳
  void enable_heartbeat(bool enable) {
    if (abandoned_)
      return;
    heartbear_enabled_ = enable;
    if (!enable) {
      scheduler_->cancel(heartbeat_task_);
    }
  }
  ulong get_next_log_idx() const { return next_log_idx_; }
  void set_next_log_idx(ulong idx) { next_log_idx_ = idx; }
  uint64_t get_last_accepted_log_idx() const { return last_accepted_log_idx_; }
  void set_last_accepted_log_idx(uint64_t to) { last_accepted_log_idx_.store(to); }
  int64 get_next_batch_size_hint_in_bytes() const { return next_batch_size_hint_in_bytes_; }
  void set_next_batch_size_hint_in_bytes(int64 batch_size) { next_batch_size_hint_in_bytes_.store(batch_size); }
  ulong get_matched_idx() const { return matched_idx_; }
  void set_matched_idx(ulong idx) { matched_idx_ = idx; }
  void set_pending_commit() { pending_commit_flag_.store(true); }
  bool clear_pending_commit() {
    bool t = true;
    return pending_commit_flag_.compare_exchange_strong(t, false);
  }
  void set_snapshot_in_sync(const ptr<snapshot> &s, ulong timeout_ms = 10 * 1000) {
    std::lock_guard guard{snp_sync_ctx_lock_};
    if (s == nullptr) {
      snp_sync_ctx_.reset();
    } else {
      snp_sync_ctx_ = new_ptr<snapshot_sync_ctx>(s, get_id(), timeout_ms);
    }
  }
  ptr<snapshot_sync_ctx> get_snapshot_sync_ctx() const {
    std::lock_guard<std::mutex> guard{snp_sync_ctx_lock_};
    return snp_sync_ctx_;
  }
  void slow_down_heartbeat() {
    current_heartbeat_interval_ = std::min(max_heartbeat_interval_, current_heartbeat_interval_ + rpc_backoff_);
  }
  void resume_heartbeat_speed() { current_heartbeat_interval_ = heartbeat_interval_; }
  void set_heartbeat_interval(int32 new_interval) { heartbeat_interval_ -= new_interval; }
  void send_req(ptr<peer> myself, ptr<req_msg> &req, rpc_handler &handler);
  void shutdown();
  void reset_last_sent_timer() { last_sent_timer_.reset(); }
  uint64_t get_last_sent_timer_us() { return last_sent_timer_.get_us(); }
  void reset_resp_timer() { last_resp_timer_.reset(); }
  uint64_t get_resp_timer_us() { return last_resp_timer_.get_us(); }
  void reset_active_timer() { last_active_timer_.reset(); }
  uint64_t get_active_timer_us() { return last_active_timer_.get_us(); }
  void reset_long_pause_warnings() { long_pause_warnings_.store(0); }
  void inc_long_pause_warnings() { long_pause_warnings_.fetch_add(1); }
  int32 get_long_pause_warnings() { return long_pause_warnings_; }
  void reset_recovery_cnt() { network_recoveries_ = 0; }
  void inc_recovery_cnt() { network_recoveries_.fetch_add(1); }
  int32 get_recovery_cnt() const { return network_recoveries_; }
  void reset_manual_free() { manual_free_ = 0; }
  void set_manual_free() { manual_free_ = 1; }
  bool is_manual_free() { return manual_free_; }
  bool recreate_rpc(ptr<srv_config> &config, context &ctx);
  void reset_rpc_errs() { rpc_errs_ = 0; }
  void inc_rpc_errs() { rpc_errs_.fetch_add(1); }
  int32 get_rpc_errs() { return rpc_errs_; }
  void set_last_sent_idx(ulong to) { last_sent_idx_ = to; }
  ulong get_last_sent_idx() const { return last_sent_idx_.load(); }
  void reset_cnt_not_applied() { cnt_not_applied_ = 0; }
  int32 inc_cnt_not_applied() {
    cnt_not_applied_.fetch_add(1);
    return cnt_not_applied_.load();
  }
  int32 get_cnt_not_applied() { return cnt_not_applied_.load(); }
  void step_down() { stepping_down_.store(true); }
  bool is_stepping_down() const { return stepping_down_.load(); }
  void set_leave_flag() { leave_requested_ = true; }
  bool is_leave_flag_set() const { return leave_requested_.load(); }
  void inc_heartbeat_cnt_since_leave() { heartbeat_cnt_since_leave_.fetch_add(1); }
  int32 get_heartbeat_cnt_since_leave() const { return heartbeat_cnt_since_leave_.load(); }
  void schedule_reconnection() {
    reconn_timer_.set_duration_sec(3);
    reconn_timer_.reset();
    reconn_scheduled_ = true;
  }
  void clear_connection() { reconn_scheduled_ = false; }
  bool need_to_reconnect() {
    if (abandoned_)
      return false;
    if (reconn_scheduled_ && reconn_timer_.timeout()) {
      return true;
    }
    {
      std::lock_guard lock{rpc_protector_};
      if (!rpc_.get()) {
        return true;
      }
    }
    return false;
  }
  void set_suppress_following_error() { suppress_following_error_ = true; }
  bool need_to_suppress_error() {
    bool exp = true, desired = false;
    return suppress_following_error_.compare_exchange_strong(exp, desired);
  }
  void set_rsv_msg(const ptr<req_msg> &m, const rpc_handler &h) {
    rsv_msg_ = m;
    rsv_msg_handler_ = h;
  }
  ptr<req_msg> get_rsv_msg() const { return rsv_msg_; }
  rpc_handler get_rsv_msg_handler() const { return rsv_msg_handler_; }

private:
  void handle_rpc_result(ptr<peer> myself, ptr<rpc_client> my_rpc_client, ptr<req_msg> &req,
                         ptr<rpc_result> &pending_result, ptr<resp_msg> &resp, ptr<rpc_exception> &err);
  ptr<srv_config> config_;
  ptr<delayed_task_scheduler> scheduler_;
  ptr<rpc_client> rpc_;
  std::mutex rpc_protector_;
  std::atomic<int32> current_heartbeat_interval_;
  int32 heartbeat_interval_;
  int32 rpc_backoff_;
  int32 max_heartbeat_interval_;
  std::atomic<ulong> next_log_idx_;
  std::atomic<uint64_t> last_accepted_log_idx_;
  std::atomic<int64> next_batch_size_hint_in_bytes_;
  ulong matched_idx_;
  std::atomic<bool> busy_flag_;
  std::atomic<bool> pending_commit_flag_;
  bool heartbear_enabled_;
  ptr<delayed_task> heartbeat_task_;
  ptr<snapshot_sync_ctx> snp_sync_ctx_;
  mutable std::mutex snp_sync_ctx_lock_;
  std::mutex lock_;
  timer_helper last_sent_timer_;   // 上一次请求发送时的timestamp
  timer_helper last_resp_timer_;   // 上一个成功接收的响应的时间戳
  timer_helper last_active_timer_; // 上一个活跃的网络活动检测到时的时间戳
  std::atomic<int32> long_pause_warnings_;
  std::atomic<int32> network_recoveries_;
  std::atomic<bool> manual_free_;
  std::atomic<int32> rpc_errs_;
  std::atomic<ulong> last_sent_idx_; // 上一次发送的日志索引的起点
  std::atomic<int32> cnt_not_applied_;
  std::atomic<bool> leave_requested_;
  std::atomic<int32> heartbeat_cnt_since_leave_; // 心跳时间
  std::atomic<bool> stepping_down_;
  std::atomic<bool> reconn_scheduled_;
  timer_helper reconn_timer_;
  timer_helper reconn_backoff_;
  std::atomic<bool> suppress_following_error_;
  std::atomic<bool> abandoned_;
  ptr<req_msg> rsv_msg_;
  rpc_handler rsv_msg_handler_;
  ptr<logger> logger_;
};
} // namespace raft
