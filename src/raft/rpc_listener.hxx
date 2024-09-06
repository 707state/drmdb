
#ifndef _RPC_LISTENER_HXX_
#define _RPC_LISTENER_HXX_

#include "pp_util.hxx"
#include "ptr.hxx"
namespace nuraft {

// for backward compatibility
class raft_server;
typedef raft_server msg_handler;

class rpc_listener {
    __interface_body__(rpc_listener);

public:
    virtual void listen(ptr<msg_handler>& handler) = 0;
    virtual void stop() = 0;
    virtual void shutdown() {}
};

} // namespace nuraft

#endif //_RPC_LISTENER_HXX_
