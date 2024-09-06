#pragma once

#include "buffer.hxx"
#include "ptr.hxx"

namespace nuraft {

class custom_notification_msg {
public:
    enum type {
        out_of_log_range_warning    = 1,
        leadership_takeover         = 2,
        request_resignation         = 3,
    };

    custom_notification_msg(type t = out_of_log_range_warning)
        : type_(t)
        , ctx_(nullptr)
        {}

    static ptr<custom_notification_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;

    type type_;

    ptr<buffer> ctx_;
};

class out_of_log_msg {
public:
    out_of_log_msg()
        : start_idx_of_leader_(0)
        {}

    static ptr<out_of_log_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;

    ulong start_idx_of_leader_;
};

class force_vote_msg {
public:
    force_vote_msg() {}

    static ptr<force_vote_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;
};

} // namespace nuraft;

