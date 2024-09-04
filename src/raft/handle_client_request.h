#pragma once
#include "event_awaiter.h"
#include "raft/raft_server.h"
namespace raft {

struct raft_server::commit_ret_elem {
    commit_ret_elem()
        : ret_value_(nullptr)
        , result_code_(cmd_result_code::OK)
        , async_result_(nullptr)
        , callback_invoked_(false) {}

    ~commit_ret_elem() {}

    ulong idx_;
    EventAwaiter awaiter_;
    timer_helper timer_;
    ptr<buffer> ret_value_;
    cmd_result_code result_code_;
    ptr<cmd_result<ptr<buffer>>> async_result_;
    bool callback_invoked_;
};

} // namespace raft
