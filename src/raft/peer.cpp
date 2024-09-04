#include "raft/peer.h"
#include "context.h"
#include "debugging_options.h"
#include "msg_type.h"
#include "ptr.h"
#include "raft_server.h"
#include "req_msg.h"
#include "rpc_cli.h"
#include "rpc_cli_factory.h"
#include "rpc_exception.h"
#include "srv_config.h"
#include "utils.h"
#include <atomic>
#include <boost/unordered_set.hpp>
#include <raft/logger.h>
#include <raft/resp_msg.h>
#include <raft/rpc_exception.h>
#include <raft/tracer.h>
namespace raft {
void peer::send_req(ptr<peer> self, ptr<req_msg>& req, rpc_handler& handler) {
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request", config_->get_id());
        return;
    }
    if (req) {
        p_tr("send req %d to %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string(req->get_type()).c_str());
    }
    ptr<rpc_result> pending = new_ptr<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    {
        std::lock_guard l{rpc_protector_};
        if (!rpc_) {
            p_tr("rpc local is null");
            set_free();
            return;
        }
        rpc_local = rpc_;
    }
    // 采用lambda简化绑定
    rpc_handler h = [this, self, rpc_local, &req, &pending](auto&& result, auto&& error) {
        this->handle_rpc_result(self,
                                rpc_local,
                                req,
                                pending,
                                std::forward<decltype(result)>(result),
                                std::forward<decltype(error)>(error));
    };
    if (rpc_local) {
        rpc_local->send(req, h);
    }
}
void peer::handle_rpc_result(ptr<peer> myself,
                             ptr<rpc_client> my_rpc_client,
                             ptr<req_msg>& req,
                             ptr<rpc_result>& pending_result,
                             ptr<resp_msg>& resp,
                             ptr<rpc_exception>& exception) {
    boost::unordered_set<int> msg_types_to_free({msg_type::append_entries_request,
                                                 msg_type::install_snapshot_request,
                                                 msg_type::request_vote_request,
                                                 msg_type::pre_vote_request,
                                                 msg_type::leave_cluster_request,
                                                 msg_type::custom_notification_request,
                                                 msg_type::reconnect_request,
                                                 msg_type::priority_change_request});
    if (abandoned_) {
        p_in("peer %d has been shut down, ignore response.", config_->get_id());
        return;
    }
    if (req) {
        p_tr("resp of req %d -> %d, type %s, %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string(req->get_type()).c_str(),
             (exception) ? exception->what() : "OK");
    }
    if (exception == nullptr) {
        // Succeeded.
        {
            std::lock_guard<std::mutex> l(rpc_protector_);
            // The same as below, freeing busy flag should be done
            // only if the RPC hasn't been changed.
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id != given_rpc_id) {
                p_wn("[EDGE CASE] got stale RPC response from %d: "
                     "current %p (%zu), from parameter %p (%zu). "
                     "will ignore this response",
                     config_->get_id(),
                     rpc_.get(),
                     cur_rpc_id,
                     my_rpc_client.get(),
                     given_rpc_id);
                return;
            }
            // WARNING:
            //   `set_free()` should be protected by `rpc_protector_`, otherwise
            //   it may free the peer even though new RPC client is already created.
            if (msg_types_to_free.find(req->get_type()) != msg_types_to_free.end()) {
                set_free();
            }
        }
        reset_active_timer();
        {
            auto_lock(lock_);
            resume_heartbeat_speed();
        }
        ptr<rpc_exception> no_except;
        resp->set_peer(myself);
        pending_result->set_result(resp, no_except);
        reconn_backoff_.reset();
        reconn_backoff_.set_duration_ms(1);
    } else {
        // NOTE: Explicit failure is also treated as an activity
        //       of that connection.
        reset_active_timer();
        {
            auto_lock(lock_);
            slow_down_heartbeat();
        }
        ptr<resp_msg> no_resp;
        pending_result->set_result(no_resp, exception);
        {
            std::lock_guard<std::mutex> l(rpc_protector_);
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id == given_rpc_id) {
                rpc_.reset();
                if (msg_types_to_free.find(req->get_type()) != msg_types_to_free.end()) {
                    set_free();
                }

            } else {
                // WARNING (MONSTOR-9378):
                //   RPC client has been reset before this request returns
                //   error. Those two are different instances and we
                //   SHOULD NOT reset the new one.
                p_wn("[EDGE CASE] RPC for %d has been reset before "
                     "returning error: current %p (%zu), from parameter %p (%zu)",
                     config_->get_id(),
                     rpc_.get(),
                     cur_rpc_id,
                     my_rpc_client.get(),
                     given_rpc_id);
            }
        }
    }
}
bool peer::recreate_rpc(ptr<srv_config>& config, context& ctx) {
    if (abandoned_) {
        p_tr("peer %d is abandoned", config->get_id());
        return false;
    }
    ptr<rpc_client_factory> factory = nullptr;
    {
        std::lock_guard lock{ctx.ctx_lock_};
        factory = ctx.rpc_cli_factory_;
    }
    if (!factory) {
        p_tr("client factory is empty");
        return false;
    }
    std::lock_guard l_{rpc_protector_};
    bool backoff_timer_disabled =
        debugging_options::get_instance().disable_reconn_backoff_.load(
            std::memory_order::relaxed);
    if (backoff_timer_disabled) {
        p_tr("reconnection back-off timer is disabled");
    }
    if (backoff_timer_disabled || reconn_backoff_.timeout()) {
        reconn_backoff_.reset();
        size_t new_duration_ms = reconn_backoff_.get_duration_us() / 1000;
        new_duration_ms = std::min(heartbeat_interval_, (int32)new_duration_ms * 2);
        if (!new_duration_ms) new_duration_ms = 1;
        reconn_backoff_.set_duration_ms(new_duration_ms);

        rpc_ = factory->create_client(config->get_endpoint());
        p_tr("%p reconnect peer %zu", rpc_.get(), config_->get_id());

        // WARNING:
        //   A reconnection attempt should be treated as an activity,
        //   hence reset timer.
        reset_active_timer();

        set_free();
        set_manual_free();
        return true;
    } else {
        p_tr("skip reconnect this time");
    }
    return false;
}
void peer::shutdown() {
    // Should set the flag to block all incoming requests.
    abandoned_ = true;

    // Cut off all shared pointers related to ASIO and Raft server.
    scheduler_.reset();
    { // To guarantee atomic reset
        // (race between send_req()).
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
    }
    heartbeat_task_.reset();
}

} // namespace raft
