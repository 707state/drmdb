#pragma once
#include "raft/delayed_task.h"
#include "raft/ptr.h"
#include "raft/utils.h"
namespace raft {
class delayed_task_scheduler {
  __interface_body(delayed_task_scheduler);

public:
  virtual void schedule(ptr<delayed_task> &task, int32 milliseconds) = 0;
  void cancel(ptr<delayed_task> &task) {
    cancel_impl(task);
    task->cancel();
  }

private:
  virtual void cancel_impl(ptr<delayed_task> &task) = 0;
};
} // namespace raft
