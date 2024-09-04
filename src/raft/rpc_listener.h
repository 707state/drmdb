#pragma once
#include "ptr.h"
#include "utils.h"
namespace raft {
class raft_server;
using msg_handler = raft_server;

class rpc_listener {
    __interface_body(rpc_listener);

public:
    virtual void listen(ptr<msg_handler>& handler) = 0;
    virtual void stop() = 0;
    virtual void shutdown() {}
};

} // namespace raft
