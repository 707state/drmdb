#pragma once
#include "raft_server.h"
#include "rpc_cli.h"
namespace raft {
class raft_server_handler {
protected:
  static ptr<resp_msg> process_req(raft_server *srv, req_msg &req,
                                   const raft_server::req_ext_params &ext_params = raft_server::req_ext_params()) {
    return srv->process_req(req, ext_params);
  }
};
} // namespace raft
