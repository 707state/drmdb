#pragma once
#include "in_memory_log_store.h"
#include "raft/cluster_config.hxx"
#include "raft/srv_config.hxx"
#include "raft/srv_state.hxx"
#include "raft/state_mgr.hxx"
#include "server/in_memory_state_mgr.h"
namespace nuraft {

class inmem_state_mgr : public state_mgr {
public:
    inmem_state_mgr(int srv_id, const std::string& endpoint)
        : my_id_(srv_id)
        , my_endpoint_(endpoint)
        , cur_log_store_(new_ptr<inmem_log_store>()) {
        my_srv_config_ = new_ptr<srv_config>(srv_id, endpoint);

        // Initial cluster config: contains only one server (myself).
        saved_config_ = new_ptr<cluster_config>();
        saved_config_->get_servers().push_back(my_srv_config_);
    }

    ~inmem_state_mgr() {}

    ptr<cluster_config> load_config() {
        // Just return in-memory data in this example.
        // May require reading from disk here, if it has been written to disk.
        return saved_config_;
    }

    void save_config(const cluster_config& config) {
        // Just keep in memory in this example.
        // Need to write to disk here, if want to make it durable.
        ptr<buffer> buf = config.serialize();
        saved_config_ = cluster_config::deserialize(*buf);
    }

    void save_state(const srv_state& state) {
        // Just keep in memory in this example.
        // Need to write to disk here, if want to make it durable.
        ptr<buffer> buf = state.serialize();
        saved_state_ = srv_state::deserialize(*buf);
    }

    ptr<srv_state> read_state() {
        // Just return in-memory data in this example.
        // May require reading from disk here, if it has been written to disk.
        return saved_state_;
    }

    ptr<log_store> load_log_store() { return cur_log_store_; }

    int32 server_id() { return my_id_; }

    void system_exit(const int exit_code) {}

    ptr<srv_config> get_srv_config() const { return my_srv_config_; }

private:
    int my_id_;
    std::string my_endpoint_;
    ptr<inmem_log_store> cur_log_store_;
    ptr<srv_config> my_srv_config_;
    ptr<cluster_config> saved_config_;
    ptr<srv_state> saved_state_;
};

}; // namespace nuraft
