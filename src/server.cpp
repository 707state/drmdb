#include "cxxopts.hpp"
#include "index/ix_manager.h"
#include "parser/ast.h"
#include "parser/yacc.tab.hpp"
#include "raft/buffer.hxx"
#include "raft/buffer_serializer.hxx"
#include "raft/ptr.hxx"
#include "raft/raft_params.hxx"
#include "raft/srv_config.hxx"
#include "raft/state_machine.hxx"
#include "server/in_memory_state_mgr.h"
#include "server/logger_wrapper.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/txn_defs.h"
#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/charconv/from_chars.hpp>
#include <boost/integer_fwd.hpp>
#include <boost/lexical_cast.hpp>
#include <cassert>
#include <chrono>
#include <csetjmp>
#include <cstddef>
#include <exception>
#include <execution/execution_manager.h>
#include <functional>
#include <iostream>
#include <netdb.h>
#include <optimizer/optimizer.h>
#include <parser/parser.h>
#include <raft/nuraft.hxx>
#include <raft/raft_server.hxx>
#include <record/rm_manager.h>
#include <recovery/log_manager.h>
#include <recovery/log_recovery.h>
#include <server/db_state_machind.h>
#include <server/portal.h>
#include <server/server_params.h>
#include <string>
#include <thread>
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
bool do_cmd(const std::vector<std::string>& tokens);
// 实现设置回调的函数
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
    std::string prompt = "drmdb" + std::to_string(stuff.server_id_) + "> ";
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
    // heartbeat: 100 ms, election timeout: 200 - 400 ms.
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    if (!stuff.raft_instance_) {
        std::cerr << "Failed to initialize launcher (see the message "
                     "in the log file)."
                  << std::endl;
        log_wrap.reset();
        exit(-1);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Wait until Raft server is ready (upto 5 seconds).
    const size_t MAX_TRY = 100;
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

void usage() {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "drmdb <server id> <IP address and port>";
    ss << std::endl;

    std::cout << ss.str();
    exit(0);
}
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
using namespace nuraft;
using boost::asio::steady_timer;
// 构建全局所需的管理器对象
auto disk_manager = std::make_shared<DiskManager>();
auto buffer_pool_manager =
    std::make_shared<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager);
auto rm_manager = std::make_shared<RmManager>(disk_manager, buffer_pool_manager);
auto ix_manager = std::make_shared<IxManager>(disk_manager, buffer_pool_manager);
auto sm_manager = std::make_shared<SmManager>(
    disk_manager, buffer_pool_manager, rm_manager, ix_manager);
auto lock_manager = std::make_shared<LockManager>();
auto txn_manager =
    std::make_shared<TransactionManager>(lock_manager.get(), sm_manager.get());
auto ql_manager = std::make_shared<QlManager>(sm_manager.get(), txn_manager.get());
auto log_manager = std::make_shared<LogManager>(disk_manager);
auto recovery =
    std::make_shared<RecoveryManager>(disk_manager, buffer_pool_manager, sm_manager);
auto planner = std::make_shared<Planner>(sm_manager);
auto optimizer = std::make_shared<Optimizer>(sm_manager, planner);
auto portal = std::make_shared<Portal>(sm_manager.get());
auto analyze = std::make_shared<Analyze>(sm_manager.get(), disk_manager.get());

static const raft_params::return_method_type CALL_TYPE = raft_params::blocking;
void handle_result(nuraft::ptr<steady_timer> timer,
                   nuraft::raft_result& result,
                   ptr<std::exception>& exception) {
    if (result.get_result_code() == cmd_result_code::OK) {
        std::cout << "failed: " << result.get_result_code() << ", "
                  << boost::lexical_cast<std::string>(
                         timer->expiry().time_since_epoch().count())
                  << std::endl;
        return;
    }
}
void append_log(const std::string& cmd, const std::vector<std::string>& tokens) {
    if (tokens.size() < 2) {
        std::cout << "too few arguments" << std::endl;
        return;
    }
    std::string cascaded_str;
    for (size_t ii = 1; ii < tokens.size(); ++ii) {
        cascaded_str += tokens[ii] + " ";
    }
    ptr<buffer> new_log = buffer::alloc(
        sizeof(int) + cascaded_str.size()); // 新建日志包含4字节的长度和字符串的数据
    buffer_serializer bs(new_log);
    bs.put_str(cascaded_str);
    boost::asio::io_context ioc;
    ptr<steady_timer> timer = new_ptr<steady_timer>(ioc, std::chrono::milliseconds(500));
    ptr<raft_result> ret = stuff.raft_instance_->append_entries({new_log});
    if (!ret->get_accepted()) {
        std::cout << "failed to replicate: " << ret->get_result_code() << ", "
                  << boost::lexical_cast<std::string>(
                         timer->expiry().time_since_epoch().count())
                  << std::endl;
        return;
    }
    // 日志追加被接受，但不代表日志被提交了
    if (nuraft::CALL_TYPE == raft_params::blocking) {
        ptr<std::exception> err(nullptr);
        handle_result(timer, *ret, err);
    } else if (nuraft::CALL_TYPE == raft_params::async_handler) {
        ret->when_ready(std::bind(
            handle_result, timer, std::placeholders::_1, std::placeholders::_2));
    } else {
        assert(0);
    }
}
void print_status(const std::string& cmd, const std::vector<std::string>& tokens) {
    ptr<log_store> ls = stuff.smgr_->load_log_store();
    std::cout << "my server id: " << stuff.server_id_ << std::endl
              << "leader id: " << stuff.raft_instance_->get_leader() << std::endl
              << "Raft log range: " << ls->start_index() << " - " << (ls->next_slot() - 1)
              << std::endl
              << "last committed index: " << stuff.raft_instance_->get_committed_log_idx()
              << std::endl;
}
// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t* txn_id, ptr<Context> context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr
        || context->txn_->get_state() == TransactionState::COMMITTED
        || context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin();
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
    }
}
bool do_cmd(const std::vector<std::string>& tokens) {
    int offset = 0;
    txn_id_t txn_id = INVALID_TXN_ID;
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    const auto& cmd_type = tokens[0];
    if (cmd_type == "st" || cmd_type == "stat") {
        print_status(cmd_type, tokens);
        return false;
    } else if (cmd_type == "list" || cmd_type == "ls") {
        server_list(cmd_type, tokens);
        return false;
    } else if (cmd_type == "add") {
        add_server(cmd_type, tokens);
        return false;
    } else if (cmd_type == "q" || cmd_type == "quit") {
        stuff.launcher_.shutdown();
        return false;
    } else if (cmd_type == "help") {
        usage();
        return false;
    } else {
        auto cmd = fmt::format("{}", fmt::join(tokens, " "));
        if (cmd_type == "exit") {
            stuff.launcher_.shutdown();
            return false;
        } else {
            append_log(cmd_type, tokens);
        }
    }
}

using namespace drmdb_server;

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8
static bool should_exit = false;
pthread_mutex_t* buffer_mutex;
pthread_mutex_t* sockfd_mutex;

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    should_exit = true;
    log_manager->flush_log_to_disk();
    std::cout << "[External] Received Stop Signal. Server shutdown." << std::endl;
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t* txn_id, Context* context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr
        || context->txn_->get_state() == TransactionState::COMMITTED
        || context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin();
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
    }
}

void* client_handler(void* sock_fd) {
    int fd = *(static_cast<int*>(sock_fd));
    delete static_cast<int*>(sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes = 0;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;

    std::string output =
        "[External] Established client connection, Sock FD: " + std::to_string(fd) + "\n";
    std::cout << output;

    while (true) {
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        if (i_recvBytes == 0) {
            std::cout << "[External] Maybe the client has closed" << std::endl;
            break;
        }
        if (i_recvBytes == -1) {
            std::cout << "[External] Client read error!" << std::endl;
            break;
        }

        if (strcmp(data_recv, "exit") == 0) {
            std::cout << "[External] Client exit." << std::endl;
            break;
        }
        if (strcmp(data_recv, "crash") == 0) {
            std::cout << "[External] Server crash" << std::endl;
            exit(1);
        }

        std::cout << "[SQL][FD " << fd << "] " << data_recv << std::endl;

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        Context* context = new Context(
            lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
        SetTransaction(&txn_id, context);

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;
        pthread_mutex_lock(buffer_mutex);
        auto result = tokenize(data_recv);
        do_cmd(result);
        YY_BUFFER_STATE buf = yy_scan_string(data_recv);
        if (yyparse() == 0) {
            if (ast::parse_tree != nullptr) {
                try {
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf);
                    finish_analyze = true;
                    pthread_mutex_unlock(buffer_mutex);
                    // 优化器
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
                    portal->run(portalStmt, ql_manager.get(), &txn_id, context);
                    portal->drop();
                } catch (TransactionAbortException& e) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 回滚事务
                    txn_manager->abort(context->txn_, log_manager.get());
                    std::cout << "[TXN][FD " << fd << "]" << e.GetInfo() << std::endl;

                    std::fstream outfile;
                    if (sm_manager.get()->enable_output_) {
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << str;
                        outfile.close();
                    }
                } catch (DRMDBError& e) {
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    std::cerr << e.what() << std::endl;

                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    // 将报错信息写入output.txt
                    std::fstream outfile;
                    if (sm_manager.get()->enable_output_) {
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << "failure\n";
                        outfile.close();
                    }
                }
            }
        }
        if (finish_analyze == false) {
            yy_delete_buffer(buf);
            pthread_mutex_unlock(buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
        // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        if (context->txn_->get_txn_mode() == false) {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }

        delete context;
    }
    // Clear
    std::cout << "[External] Terminating current client connection..." << std::endl;
    close(fd);          // close a file descriptor.
    pthread_exit(NULL); // terminate calling thread!
}

void start_server() {
    // init mutex
    buffer_mutex = static_cast<pthread_mutex_t*>(malloc(sizeof(pthread_mutex_t)));
    sockfd_mutex = static_cast<pthread_mutex_t*>(malloc(sizeof(pthread_mutex_t)));
    LoadExecutor::load_pool_access_lock =
        static_cast<pthread_mutex_t*>(malloc(sizeof(pthread_mutex_t)));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);
    pthread_mutex_init(LoadExecutor::load_pool_access_lock, nullptr);

    int sockfd_server = 0;
    int fd_temp = 0;
    struct sockaddr_in s_addr_in {};

    // 初始化连接
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0); // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(
        sockfd_server, reinterpret_cast<struct sockaddr*>(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    while (!should_exit) {
        std::cout << "Waiting for new connection..." << std::endl;
        pthread_t thread_id = 0;
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            std::cout << "Break from Server Listen Loop\n";
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server,
                            reinterpret_cast<struct sockaddr*>(&s_addr_client),
                            reinterpret_cast<socklen_t*>(&client_length));
        if (sockfd == -1) {
            std::cout << "Accept error!" << std::endl;
            continue; // ignore current socket ,continue while loop.
        }
        int* sock_fd_ptr = new int(sockfd);
        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void*)(sock_fd_ptr))
            != 0) {
            std::cout << "Create thread fail!" << std::endl;
            delete sock_fd_ptr;
            break; // break while loop
        }
    }

    // Clear
    std::cout << " Try to close all client-connection.\n";
    int ret = shutdown(sockfd_server,
                       SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1) {
        printf("%s\n", strerror(errno));
    }
    //    assert(ret != -1);
    sm_manager->close_db();
    std::cout << " DB has been closed.\n";
    std::cout << "Server shuts down." << std::endl;
}

int main(int argc, const char* argv[]) {
    cxxopts::Options options("drmdb", "基于rmdb的二次开发，初步实现了raft算法");
    options.add_options()("h,help",
                          "输出帮助信息")("p,port", "指定port", cxxopts::value<int>())(
        "i,id", "设置服务器id", cxxopts::value<int>())(
        "a,address", "设置ip", cxxopts::value<std::string>())(
        "d,database", "设置数据库名", cxxopts::value<std::string>());
    auto result = options.parse(argc, argv);
    set_server_info(result);
    if (result.count("help")) {
        std::cout << "Usage: \n"
                  << "  ./drmdb -p/--port [PORT] -a/--address [ADDRESS] -d/--database "
                     "[DATABASE] -i/--id [ID] -h/--help";
        exit(1);
    }
    if (result.count("database")) {
        auto db_name = result["database"].as<std::string>();
        if (!sm_manager->is_dir(db_name)) {
            sm_manager->create_db(db_name);
        }
        sm_manager->open_db(db_name);
    }
    std::cout << "    --  Rafted Rmdb --" << std::endl;
    std::cout << "               Version 0.1.0" << std::endl;
    std::cout << "    Server ID:    " << stuff.server_id_ << std::endl;
    std::cout << "    Endpoint:     " << stuff.endpoint_ << std::endl;
    init_raft(std::make_shared<db_state_machine>());

    signal(SIGINT, sigint_handler);
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // recovery database
        recovery->analyze();
        recovery->redo();
        recovery->undo();

        // 开启服务端，开始接受客户端连接
        start_server();
    } catch (DRMDBError& e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
    return 0;
}
