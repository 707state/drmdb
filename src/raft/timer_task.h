#pragma once
#include "delayed_task.h"
#include "raft/basic_types.h"
#include "raft/utils.h"
#include <functional>
namespace raft {
enum timer_task_type {
  election_timer = 0x1,
  heartbeat_timer = 0x2,
};
template <typename T> class timer_task : public delayed_task {
public:
  using executor = std::function<void(T)>;
  timer_task(executor &e, T ctx, int32 type = 0) : delayed_task(type), exec_(e), ctx_(ctx) {}

protected:
  void exec() override {
    if (exec_) {
      exec_(ctx_);
    }
  }

private:
  executor exec_;
  T ctx_;
};
template <> class timer_task<void> : public delayed_task {
public:
  using executor = std::function<void()>;
  explicit timer_task(executor &e, int32 type = 0) : delayed_task(type), exec_(e) {}

protected:
  void exec() override {
    if (exec_) {
      exec_();
    }
  }

private:
  executor exec_;
};
} // namespace raft
