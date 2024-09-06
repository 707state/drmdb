
#pragma once

#include "async.hxx"
#include "buffer.hxx"
#include "event_awaiter.h"
#include "internal_timer.hxx"
#include "ptr.hxx"
#include "raft_server.hxx"

namespace nuraft {

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

} // namespace nuraft
