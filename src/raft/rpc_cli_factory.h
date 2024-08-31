#pragma once
#include "ptr.h"
#include "rpc_cli.h"
#include "utils.h"
#include <string>
namespace raft {
class rpc_client_factory {
  __interface_body(rpc_client_factory);

public:
  virtual ptr<rpc_client> create_client(const std::string &endpoint) = 0;
};
} // namespace raft
