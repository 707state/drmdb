#pragma once
#include "asio_service.h"
#include "callback.h"
#include "delayed_task_scheduler.h"
#include "ptr.h"
#include "state_machine.h"
#include "state_mgr.h"
#include "utils.h"
#include <mutex>
#include <raft/raft_params.h>
#include <raft/rpc_cli_factory.h>
namespace raft {
class context {
    public:
  context(ptr<state_mgr> &mgr, ptr<state_machine> &m, ptr<rpc_listener> &listener, ptr<logger> &l,
          ptr<rpc_client_factory> &cli_factory, ptr<delayed_task_scheduler> &scheduler, const raft_params &params)
      : state_mgr_(mgr), state_machine_(m), rpc_listener_(listener), logger_(l), rpc_cli_factory_(cli_factory),
        scheduler_(scheduler), params_(new_ptr<raft_params>(params)) {}
  void set_cb_func(cb_func::func_type func) { cb_func_ = cb_func(func); }
  ptr<raft_params> get_params() const {
    std::lock_guard<std::mutex> l{ctx_lock_};
    return params_;
  }
  void set_params(ptr<raft_params> &param) {
    std::lock_guard<std::mutex> l{ctx_lock_};
    params_ = param;
  }
  __nocopy__(context);

public:
  ptr<state_mgr> state_mgr_;
  ptr<state_machine> state_machine_;
  ptr<rpc_listener> rpc_listener_;
  ptr<logger> logger_;
  ptr<rpc_client_factory> rpc_cli_factory_;
  ptr<delayed_task_scheduler> scheduler_;
  ptr<raft_params> params_;
  cb_func cb_func_;
  mutable std::mutex ctx_lock_;
};
} // namespace raft
