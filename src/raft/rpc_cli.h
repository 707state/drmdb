#pragma once
#include "raft/async.h"
#include "raft/ptr.h"
#include "raft/rpc_exception.h"
#include "req_msg.h"
#include "utils.h"
#include <cstdint>
namespace raft {
class resp_msg;
using rpc_result = async_result<ptr<resp_msg>, ptr<rpc_exception>>;
using rpc_handler = rpc_result::handler_type;
class rpc_client {
  __interface_body(rpc_client);

public:
  virtual void send(ptr<req_msg> &req, rpc_handler &when_done, uint64_t send_timeout_ms = 0) = 0;
  virtual uint64_t get_id() const = 0;
  virtual bool is_abandoned() const = 0;
};
} // namespace raft
