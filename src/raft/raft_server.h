#pragma once
#include <raft/callback.h>
#include <raft/delayed_task_scheduler.h>
class EventAwaiter;
namespace raft {
    using CbReturnCode=cb_func::ReturnCode;
    class cluster_config;
    class custom_notification_msg;
    class delayed_task_scheduler;
    class logger;
    class peer;
    class rpc_client;
    class req_msg;

}
