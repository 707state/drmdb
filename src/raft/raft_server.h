#pragma once
#include "async.h"
#include "basic_types.h"
#include "buffer.h"
#include "ptr.h"
#include "raft/internal_timer.h"
#include "raft/srv_config.h"
#include "rpc_cli.h"
#include "utils.h"
#include <atomic>
#include <boost/unordered_map.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <raft/callback.h>
#include <raft/context.h>
#include <raft/delayed_task_scheduler.h>
#include <vector>
class EventAwaiter;
namespace raft {
using CbReturnCode = cb_func::ReturnCode;
class cluster_config;
class custom_notification_msg;
class delayed_task_scheduler;
class logger;
class peer;
class rpc_client;
class req_msg;
class context;
class state_machine;
class state_mgr;
struct raft_params;
class raft_server : public std::enable_shared_from_this<raft_server> {
  friend class rmdb_raft_global_mgr;
  friend class raft_server_handler;
  friend class snapshot_io_mgr;

public:
  struct init_options {

    init_options(bool skip_initial_election_timeout, cb_func::func_type raft_callback, bool start_server_in_constructor,
                 bool test_mode_flag)
        : skip_initial_election_timeout_(skip_initial_election_timeout), raft_callback_(std::move(raft_callback)),
          start_server_in_constructor_(start_server_in_constructor), test_mode_flag_(test_mode_flag) {}
    init_options() : init_options(false, cb_func::func_type{}, true, false){};
    init_options(bool skip_initial_election_timeout, bool start_server_in_constructor, bool test_mdoe_flag)
        : init_options(skip_initial_election_timeout, cb_func::func_type{}, start_server_in_constructor,
                       test_mdoe_flag) {}
    bool skip_initial_election_timeout_;
    cb_func::func_type raft_callback_;
    bool start_server_in_constructor_;
    bool test_mode_flag_;
  };
  struct limits {
    limits()
        : pre_vote_rejection_limit_(20), warning_limit_(20), response_limit_(20), leadership_limit_(20),
          reconnect_limit_(20), leave_limit_(20), vote_limit_(20) {}
    limits(const limits &src) { *this = src; }
    limits &operator=(const limits &src) {

      pre_vote_rejection_limit_ = src.pre_vote_rejection_limit_.load();
      warning_limit_ = src.warning_limit_.load();
      response_limit_ = src.response_limit_.load();
      leadership_limit_ = src.leadership_limit_.load();
      reconnect_limit_ = src.reconnect_limit_.load();
      leave_limit_ = src.leave_limit_.load();
      vote_limit_ = src.vote_limit_.load();
      return *this;
    }
    std::atomic<int32> pre_vote_rejection_limit_;
    std::atomic<int32> warning_limit_;
    std::atomic<int32> response_limit_;
    std::atomic<int32> leadership_limit_;
    std::atomic<int32> reconnect_limit_;
    std::atomic<int32> leave_limit_;
    std::atomic<int32> vote_limit_;
  };
  raft_server(context *ctx, const init_options &opt = init_options());
  virtual ~raft_server();
  __nocopy__(raft_server);

public:
  bool is_initialized() const { return initialized_; }
  bool is_catching_up() const { return catching_up_; }
  ptr<cmd_result<ptr<buffer>>> add_srv(const srv_config &srv);
  ptr<cmd_result<ptr<buffer>>> remove_srv(const int srv_id);
  ptr<cmd_result<ptr<buffer>>> append_entries(const std::vector<ptr<buffer>> &logs);
  struct req_ext_cb_params {
    req_ext_cb_params() : log_idx_(0), log_term_(0) {}
    uint64_t log_idx_;
    uint64_t log_term_;
    void *context{nullptr};
  };
  using req_ext_cb = std::function<void(const req_ext_cb_params &)>;
  struct req_ext_params {
    req_ext_params() : expected_term_(0) {}
    req_ext_cb after_precommit_;
    uint64_t expected_term_;
    void *context{nullptr};
  };
  ptr<cmd_result<ptr<buffer>>> append_entries_ext(const std::vector<ptr<buffer>> &logs,
                                                  const req_ext_params &ext_params);
  void set_priority(const int srv_id, const int new_priority);
  void broadcast_priority_change(const int srv_id, const int new_priority);

protected:
  using peer_iterator = boost::unordered_map<int32, ptr<peer>>::const_iterator;
  struct commit_ret_elem;
  struct pre_vote_status_t {
    ulong term_;
    std::atomic<bool> done_;
    std::atomic<int32> live_;
    std::atomic<int32> dead_;
    std::atomic<int32> abandoned_;
    std::atomic<int32> quorum_reject_count_;
    std::atomic<int32> failure_count_;
  };
  struct auto_fwd_pkg;

protected:
  // virtual ptr<resp_msg> process_req(req_msg &req, const req_ext_params &ext_params);

protected:
  static const int default_snapshot_sync_block_size;
  static limits raft_limits_;
  std::thread bg_commit_thread_;
  std::thread bg_append_thread_;
  EventAwaiter *bg_append_ea_;
  std::atomic<bool> initialized_;
  std::atomic<int32> leader_;
  int32 id_;
  int32 my_priority_;     // 本节点的优先级
  int32 target_priority_; // 要投票的节点的优先级
  timer_helper priority_change_timer_;
  int32 votes_granted_;                    // 向本节点投票的节点数
  int32 voted_responded;                   // 响应了本节点的投票请求的节点数
  std::atomic<ulong> precommit_index_;     // 上一个预提交的索引
  std::atomic<ulong> leader_commit_index_; // 当前角色为跟随者是才有效
  std::atomic<ulong>
      quick_commit_index_; // 如果当前角色为跟随者且日志远远落后于领导者，就要更远的更新，这时候就需要用到这个变量
  std::atomic<ulong> sm_commit_index_;
  std::atomic<ulong> logging_sm_target_index_;
  ulong initial_commit_index_;
  std::atomic<bool> heartbeat_alive_; // 节点与领导者之间正常联系或者领导者正常
  pre_vote_status_t pre_vote_;
  bool election_completed_; // 选举是否完成
  bool config_changing_; // 配置是否完成变更，如果为true则存在没有提交的配置，这时候要拒绝配置的变更
  std::atomic<bool> catching_up_;       // 标识追赶状态，暂停日志追加
  std::atomic<bool> out_of_log_range_;  // 如果当前节点从领导者收到的日志越界了，就设为true,
                                        // 这个节点不可以发起选举，因为其日志落后于集群
  std::atomic<bool> data_fresh_;        // 判断当前Follower数据是否足够新鲜
  std::atomic<bool> stopping_;          // true则不再接收任何请求
  std::atomic<bool> commit_bg_stopped_; // 后台的提交线程是否终止
  std::atomic<bool> append_ng_stopped_; // 如上
  std::atomic<bool> write_paused_;      // 写入操作暂停
  std::atomic<bool> sm_commit_paused_;  // true则暂停状态机的commit
  std::atomic<bool> sm_commit_exec_in_progress_; // true就表明后台线程在执行状态机
  EventAwaiter *ea_follower_log_append_;
  std::atomic<bool> test_mode_flag_;
};
} // namespace raft
