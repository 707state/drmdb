#pragma once
#include "raft/basic_types.hxx"
#include "raft/buffer.hxx"
#include "raft/buffer_serializer.hxx"
#include "raft/cluster_config.hxx"
#include "raft/ptr.hxx"
#include "raft/snapshot.hxx"
#include "raft/state_machine.hxx"
#include <atomic>
#include <cstdint>
#include <iostream>
#include <ostream>
#include <string>
#include <sys/types.h>
using namespace nuraft;
namespace drmdb_server {
class db_state_machine : public state_machine {
public:
    db_state_machine()
        : last_committed_idx_(0) {}
    db_state_machine(const db_state_machine&) = default;
    db_state_machine(db_state_machine&&) = default;
    db_state_machine& operator=(const db_state_machine&) = default;
    db_state_machine& operator=(db_state_machine&&) = default;
    ~db_state_machine() {}
    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) override {
        // Extract string from `data.
        buffer_serializer bs(data);
        std::string str = bs.get_str();

        // Just print.
        std::cout << "pre_commit " << log_idx << ": " << str << std::endl;
        return nullptr;
    }
    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        buffer_serializer bs(data);
        std::string str = bs.get_str(); // 获取数据字符串
        std::cout << "commit " << log_idx << ": " << str << std::endl;
        last_committed_idx_ = log_idx;
        return nullptr;
    }
    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) override {
        last_committed_idx_ = log_idx;
    }
    void rollback(const ulong log_idx, buffer& data) override {
        buffer_serializer bs(data);
        std::string str = bs.get_str();
        std::cout << "rollback " << log_idx << ": " << str << std::endl;
    }
    int read_logical_snp_obj(snapshot& s,
                             void*& user_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj) override {
        data_out = buffer::alloc(sizeof(int32));
        buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = true;
        return 0;
    }
    void save_logical_snp_obj(snapshot& snp,
                              ulong& obj_id,
                              buffer& data_out,
                              bool is_first_obj,
                              bool is_last_obj) override {
        std::cout << "save snapshot " << snp.get_last_log_idx() << " term "
                  << snp.get_last_log_term() << " object ID " << obj_id << std::endl;
        // Request next object.
        obj_id++;
    }
    bool apply_snapshot(snapshot& s) override {
        std::cout << "apply snapshot " << s.get_last_log_idx() << " term "
                  << s.get_last_log_term() << std::endl;
        // Clone snapshot from `s`.
        {
            std::lock_guard<std::mutex> l(last_snapshot_lock_);
            ptr<buffer> snp_buf = s.serialize();
            last_snapshot_ = snapshot::deserialize(*snp_buf);
        }
        return true;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) override {}
    ptr<snapshot> last_snapshot() override {
        std::lock_guard<std::mutex> l(last_snapshot_lock_);
        return last_snapshot_;
    }
    ulong last_commit_index() override { return last_committed_idx_; }

    void create_snapshot(snapshot& s,
                         async_result<bool>::handler_type& when_done) override {
        std::cout << "create snapshot " << s.get_last_log_idx() << " term "
                  << s.get_last_log_term() << std::endl;
        // Clone snapshot from `s`.
        {
            std::lock_guard<std::mutex> l(last_snapshot_lock_);
            ptr<buffer> snp_buf = s.serialize();
            last_snapshot_ = snapshot::deserialize(*snp_buf);
        }
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);
    }

private:
    std::atomic<uint64_t> last_committed_idx_;
    ptr<snapshot> last_snapshot_;
    std::mutex last_snapshot_lock_;
};
}; // namespace drmdb_server
