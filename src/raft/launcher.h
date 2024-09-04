#pragma once
#include "asio_service.h"
#include "global_mgr.h"
#include "ptr.h"
#include "raft_params.h"
#include "raft_server.h"
#include "state_machine.h"
#include "state_mgr.h"
#include <cstddef>
namespace raft {
class raft_launcher {
public:
    raft_launcher();
    ptr<raft_server>
    init(ptr<state_machine> sm,
         ptr<state_mgr> smgr,
         ptr<logger> logger,
         int port_number,
         const asio_service::options& asio_options,
         const raft_params& params,
         const raft_server::init_options& opt = raft_server::init_options());
    bool shutdown(size_t time_limits_sec = 5);
    ptr<asio_service> get_asio_service() const { return asio_svc_; }
    ptr<rpc_listener> get_asio_rpc_listener() const { return asio_listener_; }
    ptr<raft_server> get_raft_server() const { return raft_instance_; }

private:
    ptr<asio_service> asio_svc_;
    ptr<rpc_listener> asio_listener_;
    ptr<raft_server> raft_instance_;
};
} // namespace raft
