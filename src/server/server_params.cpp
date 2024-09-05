#include "server/server_params.h"
server_params::server_params()
    : server_id_(1)
    , addr_("localhost")
    , port_(25000)
    , raft_logger_(nullptr)
    , sm_(nullptr)
    , smgr_(nullptr)
    , raft_instance_(nullptr) {}
void server_params::reset() {
    raft_logger_.reset();
    sm_.reset();
    smgr_.reset();
    raft_instance_.reset();
}
