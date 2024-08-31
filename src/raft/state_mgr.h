#pragma once
#include "basic_types.h"
#include "ptr.h"
#include "utils.h"
namespace raft {
class cluster_config;
class log_store;
class srv_state;
class state_mgr {
  __interface_body(state_mgr);

public:
  virtual ptr<cluster_config> load_config() = 0;
  virtual void save_config(const cluster_config &config) = 0;
  virtual void save_state(const srv_state &state) = 0;
  virtual ptr<srv_state> read_state() = 0;
  virtual ptr<log_store> load_log_store() = 0;
  virtual int32 server_id() = 0;
  virtual void system_exit(const int exit_code) = 0;
};
} // namespace raft
