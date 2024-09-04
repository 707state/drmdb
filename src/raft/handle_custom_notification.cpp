#include "handle_custom_notification.h"
#include "buffer.h"
#include "buffer_serializer.h"
#include "internal_timer.h"
#include "log_entry.h"
#include "msg_type.h"
#include "ptr.h"
#include "raft/logger.h"
#include "raft/tracer.h"
#include "raft_params.h"
#include "raft_server.h"
#include "req_msg.h"
#include "resp_msg.h"
#include "srv_role.h"
#include "state_machine.h"
#include <cstdint>
#include <cstring>
namespace raft {
ptr<custom_notification_msg> custom_notification_msg::deserialize(buffer& buf) {
    ptr<custom_notification_msg> ret = new_ptr<custom_notification_msg>();
    buffer_serializer bs(buf);
    auto version = bs.get_u8();
    ret->type_ = static_cast<custom_notification_msg::type>(bs.get_u8());
    size_t buf_len = 0;
    void* ptr = bs.get_bytes(buf_len);
    if (buf_len) {
        ret->ctx_ = buffer::alloc(buf_len);
        memcpy(ret->ctx_->data_begin(), ptr, buf_len);
    } else {
        ret->ctx_ = nullptr;
    }
    return ret;
}
ptr<buffer> custom_notification_msg::serialize() const {
    const uint8_t CURRENT_VERSION = 0x0;
    size_t len = sizeof(uint8_t) * 2 + sizeof(uint32_t) + ((ctx_) ? (ctx_->size()) : 0);
    ptr<buffer> ret = buffer::alloc(len);
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u8(type_);
    if (ctx_) {
        bs.put_bytes(ctx_->data_begin(), ctx_->size());
    } else {
        bs.put_u32(0);
    }
    return ret;
}
ptr<out_of_log_msg> out_of_log_msg::deserialize(buffer& buf) {
    auto ret = new_ptr<out_of_log_msg>();
    buffer_serializer bs(buf);
    auto version = bs.get_u8();
    ret->start_idx_of_leader_ = bs.get_u64();
    return ret;
}
ptr<buffer> out_of_log_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // start log index of leader    8 bytes
    size_t len = sizeof(uint8_t) + sizeof(ulong);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u64(start_idx_of_leader_);
    return ret;
}
ptr<force_vote_msg> force_vote_msg::deserialize(buffer& buf) {
    ptr<force_vote_msg> ret = new_ptr<force_vote_msg>();
    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    return ret;
}

ptr<buffer> force_vote_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // ... to be added ...

    size_t len = sizeof(uint8_t);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    return ret;
}
ptr<resp_msg> raft_server::handle_custom_notification_req(req_msg& req) {
    auto resp = new_ptr<resp_msg>(state_->get_term(),
                                  msg_type::custom_notification_response,
                                  id_,
                                  req.get_src(),
                                  log_store_->next_slot());
    resp->accept(log_store_->next_slot());
    std::vector<ptr<log_entry>>& log_entries = req.log_entries();
    if (!log_entries.size()) {
        // Empty message, just return.
        return resp;
    }
    ptr<log_entry> msg_le = log_entries[0];
    auto buffer = msg_le->get_buf_ptr();
    if (!buffer) {
        return resp;
    }
    auto msg = custom_notification_msg::deserialize(*buffer);
    switch (msg->type_) {
    case custom_notification_msg::out_of_log_range_warning: {
        return handle_out_of_log_msg(req, msg, resp);
    }
    case custom_notification_msg::leadership_takeover: {
        return handle_leadership_takeover(req, msg, resp);
    }
    case custom_notification_msg::request_resignation: {
        return handle_resignation_request(req, msg, resp);
    }
    default:
        break;
    }

    return resp;
}
ptr<resp_msg> raft_server::handle_out_of_log_msg(req_msg& req,
                                                 ptr<custom_notification_msg> msg,
                                                 ptr<resp_msg> resp) {
    static timer_helper msg_timer{5000000};
    auto log_lv = msg_timer.timeout_and_reset() ? L_WARN : L_TRACE;
    update_term(req.get_term());
    out_of_log_range_ = true;
    auto ool_msg = out_of_log_msg::deserialize(*msg->ctx_);
    p_lv(log_lv,
         "this node is out of log range. leader's start index: %zu, "
         "my last index: %zu",
         ool_msg->start_idx_of_leader_,
         log_store_->next_slot() - 1);

    // Should restart election timer to avoid initiating false vote.
    if (req.get_term() == state_->get_term() && role_ == srv_role::follower) {
        restart_election_timer();
    }
    //    //
    //    调用回调函数，通知系统该节点收到了日志超出范围的警告，并传递相关参数供上层逻辑使用。

    cb_func::Param param(id_, leader_);
    cb_func::OutOfLogRangeWarningArgs args(ool_msg->start_idx_of_leader_);
    param.ctx = &args;
    ctx_->cb_func_.call(cb_func::OutOfLogRangeWarning, &param);

    return resp;
}
ptr<resp_msg> raft_server::handle_leadership_takeover(req_msg& req,
                                                      ptr<custom_notification_msg> msg,
                                                      ptr<resp_msg> resp) {
    if (is_leader()) {
        p_er("got leadership takeover request from peer %d ",
             "I'm already a leader",
             req.get_src());
        return resp;
    }
    p_in("[LEADERSHIP TAKEOVER] got request");
    initiate_vote(true); // 初始化强制选举
    if (role_ != srv_role::leader) {
        // 重启计时器，开始选举
        restart_election_timer();
    }
    return resp;
}
ptr<resp_msg> raft_server::handle_resignation_request(req_msg& req,
                                                      ptr<custom_notification_msg> msg,
                                                      ptr<resp_msg> resp) {
    if (!is_leader()) { // 不是领导者，不用辞职
        p_er("got resignation request from peer %d",
             "but I'm not a leader",
             req.get_src());
        return resp;
    }
    p_in("[RESIGNATION REQUEST] got request");

    yield_leadership(false, req.get_src());
    return resp;
}
void raft_server::handle_custom_notification_resp(resp_msg& resp) {
    if (!resp.get_accepted()) return;
    peer_iterator it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        p_in("the response is from an unknown peer %d", resp.get_src());
        return;
    }
    ptr<peer> p = it->second;

    p->set_next_log_idx(resp.get_next_idx());
}

} // namespace raft
