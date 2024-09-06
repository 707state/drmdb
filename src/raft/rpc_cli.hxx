
#ifndef _RPC_CLI_HXX_
#define _RPC_CLI_HXX_

#include "async.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "rpc_exception.hxx"

#include <cstdint>

namespace nuraft {

class resp_msg;

using rpc_result = async_result<ptr<resp_msg>, ptr<rpc_exception>>;

using rpc_handler = rpc_result::handler_type;

class rpc_client {
    __interface_body__(rpc_client);

public:
    virtual void
    send(ptr<req_msg>& req, rpc_handler& when_done, uint64_t send_timeout_ms = 0) = 0;

    virtual uint64_t get_id() const = 0;

    virtual bool is_abandoned() const = 0;
};

} // namespace nuraft

#endif //_RPC_CLI_HXX_
