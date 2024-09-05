#pragma once
#include "raft/launcher.hxx"
#include "raft/logger.hxx"
#include "raft/ptr.hxx"
#include "raft/raft_server.hxx"
#include "raft/state_machine.hxx"
#include "raft/state_mgr.hxx"
#include <raft/pp_util.hxx>
#include <string>
struct server_params {
    server_params();
    void reset();
    int server_id_;
    std::string addr_;
    int port_;
    std::string endpoint_;
    nuraft::ptr<nuraft::logger> raft_logger_;
    nuraft::ptr<nuraft::state_machine> sm_;
    nuraft::ptr<nuraft::state_mgr> smgr_;
    nuraft::raft_launcher launcher_;
    nuraft::ptr<nuraft::raft_server> raft_instance_;
};
