#pragma once
#include "raft/ptr.h"
#include "raft/req_msg.h"
#include "raft/utils.h"
#include <exception>
#include <string>
#include <utility>
namespace raft {
class req_msg;
class rpc_exception : public std::exception {
public:
  rpc_exception(std::string err, ptr<req_msg> req) : req_(req), err_(std::move(err)) {}
  __nocopy__(rpc_exception);

public:
  ptr<req_msg> req() const { return req_; }
  const char *what() const throw() override { return err_.c_str(); }

private:
  ptr<req_msg> req_;
  std::string err_;
};
} // namespace raft
