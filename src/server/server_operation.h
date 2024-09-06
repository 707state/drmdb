#pragma once
#include "cxxopts.hpp"
#include "in_memory_state_mgr.h"
#include "logger_wrapper.h"
#include "raft/buffer.hxx"
#include "raft/ptr.hxx"
#include "raft/srv_config.hxx"
#include "raft/state_machine.hxx"
#include <boost/charconv/from_chars.hpp>
#include <cstddef>
#include <iostream>
#include <raft/raft_server.hxx>
#include <server/server_params.h>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>
using namespace nuraft;
namespace nuraft {
using raft_result = cmd_result<ptr<buffer>>;
static raft_params::return_method_type CALL_TYPE = raft_params::blocking;

} // namespace nuraft
struct server_stuff {
    server_stuff()
        : server_id_(1)
        , addr_("localhost")
        , port_(25000)
        , raft_logger_(nullptr)
        , sm_(nullptr)
        , smgr_(nullptr)
        , raft_instance_(nullptr) {}

    void reset() {
        raft_logger_.reset();
        sm_.reset();
        smgr_.reset();
        raft_instance_.reset();
    }

    // Server ID.
    int server_id_;

    // Server address.
    std::string addr_;

    // Server port.
    int port_;

    // Endpoint: `<addr>:<port>`.
    std::string endpoint_;

    // Logger.
    ptr<logger> raft_logger_;

    // State machine.
    ptr<state_machine> sm_;

    // State manager.
    ptr<state_mgr> smgr_;

    // Raft launcher.
    raft_launcher launcher_;

    // Raft server instance.
    ptr<raft_server> raft_instance_;
};
static server_stuff stuff;

void add_server(const std::string& cmd, const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) {
        std::cout << "too few arguments" << std::endl;
        return;
    }

    int server_id_to_add = atoi(tokens[1].c_str());
    if (!server_id_to_add || server_id_to_add == stuff.server_id_) {
        std::cout << "wrong server id: " << server_id_to_add << std::endl;
        return;
    }

    std::string endpoint_to_add = tokens[2];
    srv_config srv_conf_to_add(server_id_to_add, endpoint_to_add);
    ptr<raft_result> ret = stuff.raft_instance_->add_srv(srv_conf_to_add);
    if (!ret->get_accepted()) {
        std::cout << "failed to add server: " << ret->get_result_code() << std::endl;
        return;
    }
    std::cout << "async request is in progress (check with `list` command)" << std::endl;
}

void server_list(const std::string& cmd, const std::vector<std::string>& tokens) {
    std::vector<ptr<srv_config>> configs;
    stuff.raft_instance_->get_srv_config_all(configs);

    int leader_id = stuff.raft_instance_->get_leader();

    for (auto& entry: configs) {
        ptr<srv_config>& srv = entry;
        std::cout << "server id " << srv->get_id() << ": " << srv->get_endpoint();
        if (srv->get_id() == leader_id) {
            std::cout << " (LEADER)";
        }
        std::cout << std::endl;
    }
}

// 定义一个回调类型
using CommandCallback = std::function<bool(const std::vector<std::string>& tokens)>;
// extern bool (*do_cmd)(const std::vector<std::string>& tokens);
extern CommandCallback do_cmd = nullptr;
// 实现设置回调的函数
void set_command_callback(CommandCallback callback) { do_cmd = callback; }
std::vector<std::string> tokenize(const char* str, char c = ' ') {

    std::vector<std::string> tokens;
    do {
        const char* begin = str;
        while (*str != c && *str)
            str++;
        if (begin != str) tokens.push_back(std::string(begin, str));
    } while (0 != *str++);

    return tokens;
}

void loop() {
    char cmd[1000];
    std::string prompt = "calc " + std::to_string(stuff.server_id_) + "> ";
    while (true) {
        std::cout << prompt;
        if (!std::cin.getline(cmd, 1000)) {
            break;
        }

        std::vector<std::string> tokens = tokenize(cmd);
        bool cont = do_cmd(tokens);
        if (!cont) break;
    }
}

void init_raft(ptr<state_machine> sm_instance) {
    // Logger.
    std::string log_file_name = "./srv" + std::to_string(stuff.server_id_) + ".log";
    ptr<logger_wrapper> log_wrap = new_ptr<logger_wrapper>(log_file_name, 4);
    stuff.raft_logger_ = log_wrap;

    // State machine.
    stuff.smgr_ = new_ptr<inmem_state_mgr>(stuff.server_id_, stuff.endpoint_);
    // State manager.
    stuff.sm_ = sm_instance;

    // ASIO options.
    asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 4;

    // Raft parameters.
    raft_params params;
#if defined(WIN32) || defined(_WIN32)
    // heartbeat: 1 sec, election timeout: 2 - 4 sec.
    params.heart_beat_interval_ = 1000;
    params.election_timeout_lower_bound_ = 2000;
    params.election_timeout_upper_bound_ = 4000;
#else
    // heartbeat: 100 ms, election timeout: 200 - 400 ms.
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
#endif
    // Upto 5 logs will be preserved ahead the last snapshot.
    params.reserved_log_items_ = 5;
    // Snapshot will be created for every 5 log appends.
    params.snapshot_distance_ = 5;
    // Client timeout: 3000 ms.
    params.client_req_timeout_ = 3000;
    // According to this method, `append_log` function
    // should be handled differently.
    params.return_method_ = nuraft::CALL_TYPE;

    // Initialize Raft server.
    stuff.raft_instance_ = stuff.launcher_.init(
        stuff.sm_, stuff.smgr_, stuff.raft_logger_, stuff.port_, asio_opt, params);
    if (!stuff.raft_instance_) {
        std::cerr << "Failed to initialize launcher (see the message "
                     "in the log file)."
                  << std::endl;
        log_wrap.reset();
        exit(-1);
    }

    // Wait until Raft server is ready (upto 5 seconds).
    const size_t MAX_TRY = 20;
    std::cout << "init Raft instance ";
    for (size_t ii = 0; ii < MAX_TRY; ++ii) {
        if (stuff.raft_instance_->is_initialized()) {
            std::cout << " done" << std::endl;
            return;
        }
        std::cout << ".";
        fflush(stdout);
    }
    std::cout << " FAILED" << std::endl;
    log_wrap.reset();
    exit(-1);
}

void usage(int argc, const char* argv[]) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " <server id> <IP address and port>";
    ss << std::endl;

    std::cout << ss.str();
    exit(0);
}
// TODO: 魔改一下,要用cxxopts处理参数
void set_server_info(cxxopts::ParseResult& results) {
    // Get server ID.
    if (results.count("id")) {
        stuff.server_id_ = results["id"].as<int>();
    } else {
        stuff.server_id_ = 1;
    }
    if (stuff.server_id_ < 1) {
        std::cerr << "wrong server id (should be >= 1): " << stuff.server_id_
                  << std::endl;
    }

    // Get server address and port.
    if (results.count("address")) {
        stuff.addr_ = results["address"].as<std::string>();
    } else {
        stuff.addr_ = "127.0.0.1";
    }
    if (results.count("port")) {
        stuff.port_ = results["port"].as<int>();
    } else {
        stuff.port_ = 9999;
    }
    if (stuff.port_ < 1000) {
        std::cerr << "wrong port (should be >= 1000): " << stuff.port_ << std::endl;
    }
    stuff.endpoint_ = stuff.addr_ + ":" + std::to_string(stuff.port_);
}
