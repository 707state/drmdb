#pragma once
#include "basic_types.h"
#include "ptr.h"
#include "state_machine.h"
#include "utils.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <internal_timer.h>
#include <list>
#include <mutex>
#include <sys/types.h>
class EventAwaiter;
namespace raft {
class peer;
class raft_server;
class resp_msg;
class rpc_exception;
class snapshot;
class snapshot_sync_ctx {
public:
  snapshot_sync_ctx(const ptr<snapshot> &s, int peer_id, ulong timeout_ms, ulong offset = 0L);
  __nocopy__(snapshot_sync_ctx);

public:
  const ptr<snapshot> &get_snapshot() const { return snapshot_; }
  ulong get_offset() const { return offset_; }
  ulong get_obj_idx() const { return obj_idx_; }
  void *&get_user_snp_ctx() { return user_snp_ctx_; }
  void set_offset(ulong offset);
  void set_obj_idx(ulong obj_idx) { obj_idx_ = obj_idx; }
  void set_user_snp_ctx(void *_user_snap_ctx) { user_snp_ctx_ = _user_snap_ctx; }
  timer_helper &get_timer() { return timer_; }

private:
  void io_thread_loop();
  int32_t peer_id_;
  ptr<snapshot> snapshot_;
  union {
    ulong offset_;
    ulong obj_idx_;
  };
  void *user_snp_ctx_;
  timer_helper timer_;
};
class snapshot_io_mgr {
public:
  static snapshot_io_mgr &get_instance() {
    static snapshot_io_mgr mgr;
    return mgr;
  }
  bool push(ptr<raft_server> r, ptr<peer> p, std::function<void(ptr<resp_msg> *, ptr<rpc_exception> &)> &h);
  void invoke();
  void drop_reqs(raft_server *r);
  bool has_pending_request(raft_server *r, int srv_id);
  void shutdown();

private:
  struct io_queue_elem;
  snapshot_io_mgr();
  ~snapshot_io_mgr();
  void async_io_loop();
  bool push(ptr<io_queue_elem> &elem);
  std::thread io_thread_;
  ptr<EventAwaiter> io_thread_event_awaiter_;
  std::atomic<bool> terminating_;
  std::list<ptr<io_queue_elem>> queue_;
  std::mutex queue_lock_;
};
} // namespace raft
