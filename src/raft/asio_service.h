#pragma once
#include "asio_service_options.h"
#include "delayed_task.h"
#include "delayed_task_scheduler.h"
#include "ptr.h"
#include "rpc_cli_factory.h"
#include "utils.h"
namespace raft {
class asio_service_impl;
class logger;
class rpc_listener;
class asio_service : public delayed_task_scheduler, public rpc_client_factory {
public:
    using meta_cb_params = asio_service_meta_cb_params;
    using options = asio_service_options;
    explicit asio_service(const options& _opt = options(), ptr<logger> logger_ = nullptr);
    ~asio_service();
    __nocopy__(asio_service);

public:
    virtual void schedule(ptr<delayed_task>& task, int32 milliseconds) __override__;

    virtual ptr<rpc_client> create_client(const std::string& endpoint) __override__;

    ptr<rpc_listener> create_rpc_listener(ushort listening_port, ptr<logger>& l);

    void stop();

    uint32_t get_active_workers();

private:
    virtual void cancel_impl(ptr<delayed_task>& task) __override__;
    asio_service_impl* impl_;
    ptr<logger> logger_;
};
} // namespace raft
