#include "global_mgr.h"
#include "raft/asio_service.h"
#include "raft/event_awaiter.h"
#include "raft/logger.h"
#include "raft/raft_server.h"
#include <raft/raft_params.h>
#include <raft/tracer.h>
#include <thread>
namespace raft {
class ngm_singleton {
public:
  static ngm_singleton &get_instance() {
    static ngm_singleton instance;
    return instance;
  }

  raft_global_mgr *get() { return internal_.get(); }

  bool create() {
    if (internal_.get()) {
      // Already created.
      return false;
    }
    // C++11 doesn't have `make_unique`.
    internal_ = std::unique_ptr<raft_global_mgr>(new raft_global_mgr());
    return true;
  }

  void clear() { internal_.reset(); }

private:
  ngm_singleton() : internal_(nullptr) {}

  std::unique_ptr<raft_global_mgr> internal_;
};

struct raft_global_mgr::worker_handle {
  worker_handle(size_t id = 0) : id_(id), thread_(nullptr), stopping_(false), status_(SLEEPING) {}

  ~worker_handle() { shutdown(); }

  void shutdown() {
    stopping_ = true;
    if (thread_) {
      if (thread_->joinable()) {
        ea_.invoke();
        thread_->join();
      }
      thread_.reset();
    }
  }

  enum status {
    SLEEPING = 0,
    WORKING = 1,
  };

  size_t id_;
  EventAwaiter ea_;
  ptr<std::thread> thread_;
  std::atomic<bool> stopping_;
  std::atomic<status> status_;
};

raft_global_mgr::raft_global_mgr() : asio_service_(nullptr), thread_id_counter_(0) {}

raft_global_mgr::~raft_global_mgr() {
  for (auto &entry : append_workers_) {
    ptr<worker_handle> &wh = entry;
    wh->shutdown();
  }
  append_workers_.clear();

  for (auto &entry : commit_workers_) {
    ptr<worker_handle> &wh = entry;
    wh->shutdown();
  }
  commit_workers_.clear();
}

raft_global_mgr *raft_global_mgr::init(const raft_global_config &config) {
  raft_global_mgr *mgr = ngm_singleton::get_instance().get();
  if (!mgr) {
    bool created = ngm_singleton::get_instance().create();
    mgr = ngm_singleton::get_instance().get();
    if (created) {
      mgr->config_ = config;
      mgr->init_thread_pool();
    }
  }
  return mgr;
}

void raft_global_mgr::shutdown() { ngm_singleton::get_instance().clear(); }

raft_global_mgr *raft_global_mgr::get_instance() { return ngm_singleton::get_instance().get(); }

void raft_global_mgr::init_thread_pool() {
  for (size_t ii = 0; ii < config_.num_commit_threads_; ++ii) {
    ptr<worker_handle> w_hdl = new_ptr<worker_handle>(thread_id_counter_.fetch_add(1));
    w_hdl->thread_ = new_ptr<std::thread>(&raft_global_mgr::commit_worker_loop, this, w_hdl);
    commit_workers_.push_back(w_hdl);
  }

  for (size_t ii = 0; ii < config_.num_append_threads_; ++ii) {
    ptr<worker_handle> w_hdl = new_ptr<worker_handle>(thread_id_counter_.fetch_add(1));
    w_hdl->thread_ = new_ptr<std::thread>(&raft_global_mgr::append_worker_loop, this, w_hdl);
    append_workers_.push_back(w_hdl);
  }
}

void raft_global_mgr::init_raft_server(raft_server *server) {
  ptr<logger> &logger_ = server->logger_;
  p_in("global manager detected, %zu commit workers, %zu append workers", config_.num_commit_threads_,
       config_.num_append_threads_);
}

void raft_global_mgr::close_raft_server(raft_server *server) {
  // Cancel all requests for this raft server.
  size_t num_aborted_append = 0;
  {
    std::lock_guard<std::mutex> l(append_queue_lock_);
    auto entry = append_queue_.begin();
    while (entry != append_queue_.end()) {
      if (entry->get() == server) {
        append_server_set.erase(*entry);
        entry = append_queue_.erase(entry);
        num_aborted_append++;
        break;
      } else {
        entry++;
      }
    }
  }

  size_t num_aborted_commit = 0;
  {
    std::lock_guard<std::mutex> l(commit_queue_lock_);
    auto entry = commit_queue_.begin();
    while (entry != commit_queue_.end()) {
      if (entry->get() == server) {
        commit_server_set_.erase(*entry);
        entry = commit_queue_.erase(entry);
        num_aborted_commit++;
        break;
      } else {
        entry++;
      }
    }
  }

  ptr<logger> &logger_ = server->logger_;
  p_in("global manager detected, %zu appends %zu commits are aborted", num_aborted_append, num_aborted_commit);
}

void raft_global_mgr::request_append(ptr<raft_server> server) {
  {
    std::lock_guard<std::mutex> l(append_queue_lock_);
    // First search the set if the server is duplicate.
    auto entry = append_server_set.find(server);
    if (entry != append_server_set.end()) {
      // `server` is already in the queue. Ignore it.
      return;
    }

    // Put into queue.
    append_queue_.push_back(server);
    append_server_set.insert(server);

    ptr<logger> &logger_ = server->logger_;
    p_tr("added append request to global queue, "
         "server %p, queue length %zu",
         server.get(), append_queue_.size());
  }

  // Find a sleeping worker and invoke.
  for (auto &entry : append_workers_) {
    ptr<worker_handle> &wh = entry;
    if (wh->status_ == worker_handle::SLEEPING) {
      wh->ea_.invoke();
      break;
    }
  }
  // If all workers are working, nothing to do for now.
}

void raft_global_mgr::request_commit(ptr<raft_server> server) {
  {
    std::lock_guard<std::mutex> l(commit_queue_lock_);
    // First search the set if the server is duplicate.
    auto entry = commit_server_set_.find(server);
    if (entry != commit_server_set_.end()) {
      // `server` is already in the queue. Ignore it.
      return;
    }

    // Put into queue.
    commit_queue_.push_back(server);
    commit_server_set_.insert(server);

    ptr<logger> &logger_ = server->logger_;
    p_tr("added commit request to global queue, "
         "server %p, queue length %zu",
         server.get(), commit_queue_.size());
  }

  // Find a sleeping worker and invoke.
  for (auto &entry : commit_workers_) {
    ptr<worker_handle> &wh = entry;
    if (wh->status_ == worker_handle::SLEEPING) {
      wh->ea_.invoke();
      break;
    }
  }
  // If all workers are working, nothing to do for now.
}

void raft_global_mgr::commit_worker_loop(ptr<worker_handle> handle) {
  std::string thread_name = "nuraft_g_c" + std::to_string(handle->id_);
#ifdef __linux__
  pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
  pthread_setname_np(thread_name.c_str());
#endif

  bool skip_sleeping = false;
  while (!handle->stopping_) {
    if (!skip_sleeping) {
      handle->status_ = worker_handle::SLEEPING;
      // Wake up for every 1 second even without invoke, just in case.
      handle->ea_.wait_ms(1000);
      handle->ea_.reset();
      handle->status_ = worker_handle::WORKING;
    }
    if (handle->stopping_)
      break;

    skip_sleeping = false;
    size_t queue_length = 0;
    ptr<raft_server> target = nullptr;
    {
      std::lock_guard<std::mutex> l(commit_queue_lock_);
      auto entry = commit_queue_.begin();
      if (entry != commit_queue_.end()) {
        target = *entry;
        commit_server_set_.erase(target);
        commit_queue_.pop_front();
        queue_length = commit_queue_.size();
        if (!commit_queue_.empty()) {
          // Other requests are waiting in the queue,
          // skip sleeping next time.
          skip_sleeping = true;
        }
      }
    }
    if (!target)
      continue;

    ptr<logger> &logger_ = target->logger_;

    // Whenever we find a task to execute, skip next sleeping for any tasks
    // that can be queued in the meantime.
    skip_sleeping = true;

    p_tr("execute commit for %p", target.get());

    if (target->sm_commit_paused_) {
      p_tr("commit of this server has been paused");
      // Since there can be other Raft server waiting for being served,
      // need to skip nest sleep.
      continue;
    }

    if (target->quick_commit_index_ <= target->sm_commit_index_ ||
        target->log_store_->next_slot() - 1 <= target->sm_commit_index_) {
      // State machine's commit index is large enough not to execute commit
      // (see the comment in `commit_in_bg()`).
      continue;
    }

    p_tr("execute commit by global worker, queue length %zu", queue_length);
    bool finished_in_time = target->commit_in_bg_exec(config_.max_scheduling_uint_ms_);
    if (!finished_in_time) {
      // Commit took too long time and aborted in the middle.
      // Put this server to queue again.
      p_tr("couldn't finish in time (%zu ms), re-push to queue", config_.max_scheduling_uint_ms_);
      request_commit(target);
    } else {
      p_tr("executed in time");
    }
  }
}

void raft_global_mgr::append_worker_loop(ptr<worker_handle> handle) {
  std::string thread_name = "nuraft_g_a" + std::to_string(handle->id_);
#ifdef __linux__
  pthread_setname_np(pthread_self(), thread_name.c_str());
#elif __APPLE__
  pthread_setname_np(thread_name.c_str());
#endif

  bool skip_sleeping = false;
  while (!handle->stopping_) {
    if (!skip_sleeping) {
      handle->status_ = worker_handle::SLEEPING;
      // Ditto, just in case.
      handle->ea_.wait_ms(1000);
      handle->ea_.reset();
      handle->status_ = worker_handle::WORKING;
    }
    if (handle->stopping_)
      break;

    skip_sleeping = false;
    size_t queue_length = 0;
    ptr<raft_server> target = nullptr;
    {
      std::lock_guard<std::mutex> l(append_queue_lock_);
      auto entry = append_queue_.begin();
      if (entry != append_queue_.end()) {
        target = *entry;
        append_server_set.erase(target);
        append_queue_.pop_front();
        queue_length = append_queue_.size();
        if (!append_queue_.empty()) {
          // Other requests are waiting in the queue,
          // skip sleeping next time.
          skip_sleeping = true;
        }
      }
    }
    if (!target)
      continue;

    ptr<logger> &logger_ = target->logger_;

    // Whenever we find a task to execute, skip next sleeping for any tasks
    // that can be queued in the meantime.
    skip_sleeping = true;

    p_tr("executed append_entries by global worker, queue length %zu", queue_length);
    target->append_entries_in_bg_exec();
  }
}

} // namespace raft
