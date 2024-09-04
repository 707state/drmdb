#pragma once
#include "raft/asio_service_options.h"
#include "raft/ptr.h"
#include <atomic>
#include <boost/unordered_set.hpp>
#include <cstddef>
#include <initializer_list>
#include <list>
#include <mutex>
#include <raft/utils.h>
#include <vector>
namespace raft {
class asio_service;
class logger;
class raft_server;
struct raft_global_config {
  raft_global_config() : num_append_threads_(1), num_commit_threads_(1), max_scheduling_uint_ms_(200) {}
  size_t num_commit_threads_;
  size_t num_append_threads_;
  size_t max_scheduling_uint_ms_;
};
static raft_global_config __DEFAULT_RAFT_GLOBAL_CONFIG;
class raft_global_mgr {
public:
  raft_global_mgr();
  ~raft_global_mgr();
  __nocopy__(raft_global_mgr);

public:
  static raft_global_mgr *init(const raft_global_config &config = __DEFAULT_RAFT_GLOBAL_CONFIG);
  static void shutdown();
  static raft_global_mgr *get_instance();
  static ptr<asio_service> init_asio_service(const asio_service_options &asio_opt = asio_service_options(),
                                             ptr<logger> logger_inst = nullptr);
  static ptr<asio_service> get_asio_service();
  void init_raft_server(raft_server *server);
  void close_raft_server(raft_server *server);
  void request_append(ptr<raft_server> server);
  void request_commit(ptr<raft_server> server);

private:
  struct worker_handle;
  void init_thread_pool();
  void commit_worker_loop(ptr<worker_handle> handle);
  void append_worker_loop(ptr<worker_handle> handle);
  std::mutex asio_service_lock_;
  ptr<asio_service> asio_service_;
  raft_global_config config_;
  std::atomic<size_t> thread_id_counter_;
  std::vector<ptr<worker_handle>> commit_workers_;
  std::vector<ptr<worker_handle>> append_workers_;
  std::list<ptr<raft_server>> commit_queue_;
  boost::unordered_set<ptr<raft_server>> commit_server_set_;
  std::mutex commit_queue_lock_;
  std::list<ptr<raft_server>> append_queue_;
  boost::unordered_set<ptr<raft_server>> append_server_set;
  std::mutex append_queue_lock_;
};

} // namespace raft
