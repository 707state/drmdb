#pragma once
#include "async.h"
#include "basic_types.h"
#include "buffer.h"
#include "delayed_task.h"
#include "log_entry.h"
#include "ptr.h"
#include "raft/internal_timer.h"
#include "raft/snapshot_sync_ctx.h"
#include "raft/snapshot_sync_req.h"
#include "raft/srv_config.h"
#include "raft/srv_state.h"
#include "rpc_cli.h"
#include "state_machine.h"
#include "state_mgr.h"
#include "utils.h"
#include <atomic>
#include <boost/unordered_map.hpp>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <raft/callback.h>
#include <raft/context.h>
#include <raft/delayed_task_scheduler.h>
#include <raft/log_store.h>
#include <raft/srv_role.h>
#include <raft/timer_task.h>
#include <string>
#include <thread>
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

        init_options(bool skip_initial_election_timeout,
                     cb_func::func_type raft_callback,
                     bool start_server_in_constructor,
                     bool test_mode_flag)
            : skip_initial_election_timeout_(skip_initial_election_timeout)
            , raft_callback_(std::move(raft_callback))
            , start_server_in_constructor_(start_server_in_constructor)
            , test_mode_flag_(test_mode_flag) {}
        init_options()
            : init_options(false, cb_func::func_type{}, true, false){};
        init_options(bool skip_initial_election_timeout,
                     bool start_server_in_constructor,
                     bool test_mdoe_flag)
            : init_options(skip_initial_election_timeout,
                           cb_func::func_type{},
                           start_server_in_constructor,
                           test_mdoe_flag) {}
        bool skip_initial_election_timeout_;
        cb_func::func_type raft_callback_;
        bool start_server_in_constructor_;
        bool test_mode_flag_;
    };
    struct limits {
        limits()
            : pre_vote_rejection_limit_(20)
            , warning_limit_(20)
            , response_limit_(20)
            , leadership_limit_(20)
            , reconnect_limit_(20)
            , leave_limit_(20)
            , vote_limit_(20) {}
        limits(const limits& src) { *this = src; }
        limits& operator=(const limits& src) {

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
    raft_server(context* ctx, const init_options& opt = init_options());
    virtual ~raft_server();
    __nocopy__(raft_server);

public:
    bool is_initialized() const { return initialized_; }
    bool is_catching_up() const { return catching_up_; }
    ptr<cmd_result<ptr<buffer>>> add_srv(const srv_config& srv);
    ptr<cmd_result<ptr<buffer>>> remove_srv(const int srv_id);
    ptr<cmd_result<ptr<buffer>>> append_entries(const std::vector<ptr<buffer>>& logs);
    struct req_ext_cb_params {
        req_ext_cb_params()
            : log_idx_(0)
            , log_term_(0) {}
        uint64_t log_idx_;
        uint64_t log_term_;
        void* context{nullptr};
    };
    using req_ext_cb = std::function<void(const req_ext_cb_params&)>;
    struct req_ext_params {
        req_ext_params()
            : expected_term_(0) {}
        req_ext_cb after_precommit_;
        uint64_t expected_term_;
        void* context{nullptr};
    };
    ptr<cmd_result<ptr<buffer>>> append_entries_ext(const std::vector<ptr<buffer>>& logs,
                                                    const req_ext_params& ext_params);
    void set_priority(const int srv_id, const int new_priority);
    void broadcast_priority_change(const int srv_id, const int new_priority);
    void yield_leadership(bool immediate_yield = false, int successor_id = -1);
    bool request_leadership();
    void restart_election_timer();
    void set_user_ctx(const std::string& ctx);
    std::string get_user_ctx() const;
    int32 get_id() const { return id_; }
    ulong get_log_term(ulong log_idx) const { return log_store_->term_at(log_idx); }
    ulong get_last_log_idx() const { return log_store_->next_slot() - 1; }
    ulong get_last_log_term() const { return log_store_->term_at(get_last_log_idx()); }
    ulong get_committed_log_idx() const { return sm_commit_index_.load(); }
    // 获取要提交的日志索引
    ulong get_target_committed_log_idx() const { return quick_commit_index_.load(); }
    ulong get_leader_committed_log_idx() const {
        return is_leader() ? get_committed_log_idx() : leader_commit_index_.load();
    }
    ulong get_expected_committed_log_idx();
    ptr<cluster_config> get_config() const;
    ptr<log_store> get_log_store() const { return log_store_; }
    int32 get_dc_id(int32 srv_id) const;
    std::string get_aux(int32 srv_id) const;
    int32 get_leader() const {
        if (leader_ == id_ && role_ != srv_role::leader) return -1;
        return leader_;
    }
    bool is_leader() const {
        if (leader_ == id_ && role_ == srv_role::leader) return true;
        return false;
    }
    bool is_leader_alive() {
        if (leader_ == -1 || !heartbeat_alive_) {
            return false;
        }
        return true;
    }
    ptr<srv_config> get_srv_config(int32 srv_id) const;
    void get_srv_config_all(std::vector<ptr<srv_config>>& configure_out) const;
    struct peer_info {
        peer_info()
            : id_(-1)
            , last_log_idx_(0)
            , last_succ_resp_us_(0) {}
        int32 id_;
        ulong last_log_idx_;
        ulong last_succ_resp_us_;
    };
    peer_info get_peer_info(int32 srv_id) const;
    std::vector<peer_info> get_peer_info_all() const;
    void shutdown();
    void start_server(bool skip_initial_election_timeout);
    void stop_server();
    void send_reconnect_request();
    void update_params(const raft_params& new_params);
    raft_params get_current_params() const;
    static uint64_t get_stat_counter(const std::string& name);
    static int64_t get_stat_gauge(const std::string& name);
    static bool get_stat_histogram(const std::string& name,
                                   boost::unordered_map<double, uint64_t>& histogram_out);
    static void reset_stat(const std::string& name);
    static void reset_all_stats();
    static bool apply_config_log_entry(ptr<log_entry>& le,
                                       ptr<state_mgr>& s_mgr,
                                       std::string& err_msg);
    static limits get_raft_limits();
    static void set_raft_limits(const limits& new_limits);
    CbReturnCode invoke_callback(cb_func::Type type, cb_func::Param* param);
    void set_inc_term_func(srv_state::inc_term_func func);
    void pause_state_machine_execution(size_t timeout_ms = 0);
    void resume_state_machine_execution();
    bool is_state_machine_paused() const;
    bool wait_for_state_machine_pause(size_t timeout_ms);
    void notify_log_append_completion(bool ok);
    ulong create_snapshot();
    ulong get_last_snapshot_idx() const;

protected:
    using peer_iterator = boost::unordered_map<int32, ptr<peer>>::const_iterator;
    struct commit_ret_elem;
    struct pre_vote_status_t {
        pre_vote_status_t()
            : quorum_reject_count_(0)
            , failure_count_(0) {
            reset(0);
        }
        void reset(ulong _term) {
            term_ = _term;
            done_ = false;
            live_ = dead_ = abandoned_ = 0;
        }
        ulong term_{};
        std::atomic<bool> done_;
        std::atomic<int32> live_;
        std::atomic<int32> dead_;
        std::atomic<int32> abandoned_;
        std::atomic<int32> quorum_reject_count_;
        std::atomic<int32> failure_count_;
    };
    struct auto_fwd_pkg;

public:
    virtual ptr<resp_msg> process_req(req_msg& req, const req_ext_params& ext_params);
    void apply_and_log_current_params();
    void cancel_schedulers();
    void schedule_task(ptr<delayed_task>& task, int32 milliseconds);
    void cancel_task(ptr<delayed_task>& task);
    bool check_leadership_validity();
    void check_leadership_transfer();
    void update_rand_timeout();
    void cancel_global_requests();
    bool is_regular_member(const ptr<peer>& p);
    int32 get_num_voting_members();
    int32 get_quorum_for_election();
    int32 get_quorum_for_commit();
    int32 get_leadership_expiry();
    size_t get_not_responding_peers();
    size_t get_num_stale_peers();
    ptr<resp_msg> handle_append_entries(req_msg& req);
    ptr<resp_msg> handle_prevote_req(req_msg& req);
    ptr<resp_msg> handle_vote_req(req_msg& req);
    ptr<resp_msg> handle_cli_req_prelock(req_msg& req, const req_ext_params& ext_params);
    ptr<resp_msg>
    handle_cli_req(req_msg& req, const req_ext_params& ext_params, uint64_t timestamp_us);
    ptr<resp_msg> handle_cli_req_callback(ptr<commit_ret_elem> elem, ptr<resp_msg> resp);
    ptr<cmd_result<ptr<buffer>>>
    handle_cli_req_callback_async(ptr<cmd_result<ptr<buffer>>> async_res);

    void drop_all_pending_commit_elems();
    ptr<resp_msg> handle_ext_msg(req_msg& req);
    ptr<resp_msg> handle_install_snapshot_req(req_msg& req);
    ptr<resp_msg> handle_rm_srv_req(req_msg& req);
    ptr<resp_msg> handle_add_srv_req(req_msg& req);
    ptr<resp_msg> handle_log_sync_req(req_msg& req);
    ptr<resp_msg> handle_join_cluster_req(req_msg& req);
    ptr<resp_msg> handle_leave_cluster_req(req_msg& req);
    ptr<resp_msg> handle_priority_change_req(req_msg& req);
    ptr<resp_msg> handle_reconnect_req(req_msg& req);
    ptr<resp_msg> handle_custom_notification_req(req_msg& req);

    void handle_join_cluster_resp(resp_msg& resp);
    void handle_log_sync_resp(resp_msg& resp);
    void handle_leave_cluster_resp(resp_msg& resp);

    bool handle_snapshot_sync_req(snapshot_sync_req& req);

    bool check_cond_for_zp_election();
    void request_prevote();
    void initiate_vote(bool force_vote = false);
    void request_vote(bool force_vote);
    void request_append_entries();
    bool request_append_entries(ptr<peer> p);
    void handle_peer_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err);
    void handle_append_entries_resp(resp_msg& resp);
    void handle_install_snapshot_resp(resp_msg& resp);
    void handle_install_snapshot_resp_new_member(resp_msg& resp);
    void handle_prevote_resp(resp_msg& resp);
    void handle_vote_resp(resp_msg& resp);
    void handle_priority_change_resp(resp_msg& resp);
    void handle_reconnect_resp(resp_msg& resp);
    void handle_custom_notification_resp(resp_msg& resp);

    bool try_update_precommit_index(ulong desired, const size_t MAX_ATTEMPTS = 10);

    void handle_ext_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err);
    void handle_ext_resp_err(rpc_exception& err);
    void handle_join_leave_rpc_err(msg_type t_msg, ptr<peer> p);
    void reset_srv_to_join();
    void reset_srv_to_leave();
    ptr<req_msg> create_append_entries_req(ptr<peer>& pp);
    ptr<req_msg> create_sync_snapshot_req(ptr<peer>& pp,
                                          ulong last_log_idx,
                                          ulong term,
                                          ulong commit_idx,
                                          bool& succeeded_out);
    bool check_snapshot_timeout(ptr<peer> pp);
    void destroy_user_snp_ctx(ptr<snapshot_sync_ctx> sync_ctx);
    void clear_snapshot_sync_ctx(peer& pp);
    void commit(ulong target_idx);
    bool snapshot_and_compact(ulong committed_idx, bool forced_creation = false);
    bool update_term(ulong term);
    void reconfigure(const ptr<cluster_config>& new_config);
    void update_target_priority();
    void decay_target_priority();
    bool reconnect_client(peer& p);
    void become_leader();
    void become_follower();
    void check_srv_to_leave_timeout();
    void enable_hb_for_peer(peer& p);
    void stop_election_timer();
    void handle_hb_timeout(int32 srv_id);
    void reset_peer_info();
    void handle_election_timeout();
    void sync_log_to_new_srv(ulong start_idx);
    void invite_srv_to_join_cluster();
    void rm_srv_from_cluster(int32 srv_id);
    int get_snapshot_sync_block_size() const;
    void on_snapshot_completed(ptr<snapshot>& s, bool result, ptr<std::exception>& err);
    void on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req);
    ulong term_for_log(ulong log_idx);

    void commit_in_bg();
    bool commit_in_bg_exec(size_t timeout_ms = 0);

    void append_entries_in_bg();
    void append_entries_in_bg_exec();

    void commit_app_log(ulong idx_to_commit,
                        ptr<log_entry>& le,
                        bool need_to_handle_commit_elem);
    void commit_conf(ulong idx_to_commit, ptr<log_entry>& le);

    ptr<cmd_result<ptr<buffer>>>
    send_msg_to_leader(ptr<req_msg>& req,
                       const req_ext_params& ext_params = req_ext_params());

    void auto_fwd_release_rpc_cli(ptr<auto_fwd_pkg> cur_pkg, ptr<rpc_client> rpc_cli);

    void auto_fwd_resp_handler(ptr<cmd_result<ptr<buffer>>> presult,
                               ptr<auto_fwd_pkg> cur_pkg,
                               ptr<rpc_client> rpc_cli,
                               ptr<resp_msg>& resp,
                               ptr<rpc_exception>& err);
    void cleanup_auto_fwd_pkgs();

    void set_config(const ptr<cluster_config>& new_config);
    ptr<snapshot> get_last_snapshot() const;
    void set_last_snapshot(const ptr<snapshot>& new_snapshot);

    ulong store_log_entry(ptr<log_entry>& entry, ulong index = 0);

    ptr<resp_msg> handle_out_of_log_msg(req_msg& req,
                                        ptr<custom_notification_msg> msg,
                                        ptr<resp_msg> resp);

    ptr<resp_msg> handle_leadership_takeover(req_msg& req,
                                             ptr<custom_notification_msg> msg,
                                             ptr<resp_msg> resp);

    ptr<resp_msg> handle_resignation_request(req_msg& req,
                                             ptr<custom_notification_msg> msg,
                                             ptr<resp_msg> resp);

    void remove_peer_from_peers(const ptr<peer>& pp);

    void check_overall_status();

    void request_append_entries_for_all();

    uint64_t get_current_leader_index();

public:
    static const int default_snapshot_sync_block_size;
    static limits raft_limits_;
    std::thread bg_commit_thread_;
    std::thread bg_append_thread_;
    EventAwaiter* bg_append_ea_;
    std::atomic<bool> initialized_;
    std::atomic<int32> leader_;
    int32 id_;
    int32 my_priority_;     // 本节点的优先级
    int32 target_priority_; // 要投票的节点的优先级
    timer_helper priority_change_timer_;
    int32 votes_granted_;                // 向本节点投票的节点数
    int32 voted_responded;               // 响应了本节点的投票请求的节点数
    std::atomic<ulong> precommit_index_; // 上一个预提交的索引
    std::atomic<ulong> leader_commit_index_; // 当前角色为跟随者是才有效
    std::atomic<ulong>
        quick_commit_index_; // 如果当前角色为跟随者且日志远远落后于领导者，就要更远的更新，这时候就需要用到这个变量
    std::atomic<ulong> sm_commit_index_;
    std::atomic<ulong> logging_sm_target_index_;
    ulong initial_commit_index_;
    std::atomic<bool> heartbeat_alive_; // 节点与领导者之间正常联系或者领导者正常
    pre_vote_status_t pre_vote_;
    bool election_completed_; // 选举是否完成
    bool
        config_changing_; // 配置是否完成变更，如果为true则存在没有提交的配置，这时候要拒绝配置的变更
    std::atomic<bool> catching_up_; // 标识追赶状态，暂停日志追加
    std::atomic<bool>
        out_of_log_range_; // 如果当前节点从领导者收到的日志越界了，就设为true,
                           // 这个节点不可以发起选举，因为其日志落后于集群
    std::atomic<bool> data_fresh_;        // 判断当前Follower数据是否足够新鲜
    std::atomic<bool> stopping_;          // true则不再接收任何请求
    std::atomic<bool> commit_bg_stopped_; // 后台的提交线程是否终止
    std::atomic<bool> append_bg_stopped_; // 如上
    std::atomic<bool> write_paused_;      // 写入操作暂停
    std::atomic<bool> sm_commit_paused_;  // true则暂停状态机的commit
    std::atomic<bool> sm_commit_exec_in_progress_; // true就表明后台线程在执行状态机
    EventAwaiter* ea_follower_log_append_;
    EventAwaiter* ea_sm_commit_exec_in_progress_;

    std::atomic<int32> next_leader_candidate_;
    timer_helper reelaction_timer_;
    bool im_learner_;               // 当前节点是否为learner
    std::atomic<bool> serving_req_; // 正处于append_entries操作中
    int32 steps_to_down_;
    std::atomic<bool> snp_in_progress_;
    std::unique_ptr<context> ctx_;
    ptr<delayed_task_scheduler> scheduler_;
    timer_task<void>::executor election_exec_;
    ptr<delayed_task> election_task_;
    timer_helper last_election_timer_reset_;
    boost::unordered_map<int32, ptr<peer>> peers_;
    boost::unordered_map<int32, ptr<rpc_client>> rpc_clients_;
    boost::unordered_map<int32, ptr<auto_fwd_pkg>> auto_fwd_pkgs_;
    struct auto_fwd_req_resp {
        ptr<req_msg> req;
        ptr<cmd_result<ptr<buffer>>> resp;
    };
    std::list<auto_fwd_req_resp> auto_fwd_reqs_;
    std::mutex auto_fwg_reqs_lock_;
    std::atomic<srv_role> role_;
    ptr<srv_state> state_;
    ptr<log_store> log_store_;
    ptr<state_machine> state_machine_;
    std::atomic<bool> receiving_snapshot_;
    std::atomic<ulong> et_cnt_receiving_snapshot_;
    uint32_t first_snapshot_distance_;
    ptr<logger> logger_;
    std::function<int32()> rand_timeout_;
    ptr<cluster_config> stale_config_;
    ptr<cluster_config> config_;
    mutable std::mutex config_lock_;
    ptr<cluster_config> uncommitted_config_;
    ptr<peer> srv_to_join_;
    std::atomic<bool> srv_to_join_snp_retry_required_;
    ptr<peer> srv_to_leave_;
    ulong srv_to_leave_target_idx_;
    ptr<srv_config> conf_to_add_;
    mutable std::recursive_mutex lock_;
    std::mutex cli_lock_;               // 用于客户端请求和角色转换
    std::condition_variable commit_cv_; // 出发后台的commit线程
    std::mutex commit_cv_lock_;
    std::mutex commit_lock_;
    std::mutex rpc_clients_lock_;
    boost::unordered_map<ulong, ptr<commit_ret_elem>> coommit_ret_elems_;
    std::mutex commit_ret_elems_lock_;
    std::condition_variable ready_to_stop_cv_;
    std::mutex ready_to_stop_cv_lock_;
    rpc_handler resp_handler_;
    rpc_handler ex_resp_handler_;
    ptr<snapshot> last_snapshot_;
    mutable std::mutex last_snapshot_lock_;
    timer_helper leadership_transfer_timer_;
    timer_helper status_check_timer_; // 检查每个心跳周期的定时器
    timer_helper vote_init_timer_;
    std::atomic<ulong> vote_init_timer_term_;

    std::atomic<bool> test_mode_flag_;
};
} // namespace raft
