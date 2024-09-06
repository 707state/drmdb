#include "cxxopts.hpp"
#include "index/ix_manager.h"
#include "parser/ast.h"
#include "parser/yacc.tab.hpp"
#include "raft/buffer_serializer.hxx"
#include "raft/ptr.hxx"
#include "raft/raft_params.hxx"
#include "server/server_operation.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/txn_defs.h"
#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/integer_fwd.hpp>
#include <boost/lexical_cast.hpp>
#include <cassert>
#include <chrono>
#include <exception>
#include <execution/execution_manager.h>
#include <functional>
#include <netdb.h>
#include <optimizer/optimizer.h>
#include <parser/parser.h>
#include <raft/nuraft.hxx>
#include <record/rm_manager.h>
#include <recovery/log_manager.h>
#include <recovery/log_recovery.h>
#include <server/db_state_machind.h>
#include <server/portal.h>
#include <string>
#include <vector>
using namespace nuraft;
using boost::asio::steady_timer;
namespace drmdb_server {
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
bool execute_sql(const std::string& cmd_type, const std::vector<std::string>& tokens);
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

bool my_do_cmd(const std::vector<std::string>& tokens) {
    int offset = 0;
    txn_id_t txn_id = INVALID_TXN_ID;
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    const auto& cmd_type = tokens[0];
    if (cmd_type == "st" || cmd_type == "stat") {
        print_status(cmd_type, tokens);
    } else if (cmd_type == "list" || cmd_type == "ls") {
        server_list(cmd_type, tokens);
    } else if (cmd_type == "add") {
        add_server(cmd_type, tokens);
    } else if (cmd_type == "q" || cmd_type == "quit") {
        stuff.launcher_.shutdown();
        return false;
    } else {
        auto cmd = fmt::format("{}", fmt::join(tokens, " "));
        if (cmd_type == "exit") {
            stuff.launcher_.shutdown();
            return false;
        } else {
            append_log(cmd_type, tokens);
            // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
            memset(data_send, '\0', BUFFER_LENGTH);
            offset = 0;
            auto context = std::make_shared<Context>(
                lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
            SetTransaction(&txn_id, context);
            bool finished_analyze = false;
            YY_BUFFER_STATE buf = yy_scan_string(cmd.c_str());
            if (yyparse() == 0) {
                if (ast::parse_tree != nullptr) {
                    try {

                        std::shared_ptr<Query> query =
                            analyze->do_analyze(ast::parse_tree);
                        yy_delete_buffer(buf);
                        finished_analyze = true;
                        // 优化器
                        std::shared_ptr<Plan> plan =
                            optimizer->plan_query(query, context.get());
                        // portal
                        std::shared_ptr<PortalStmt> portalStmt =
                            portal->start(plan, context.get());
                        portal->run(portalStmt, ql_manager.get(), &txn_id, context.get());
                        portal->drop();
                    } catch (TransactionAbortException& exception) {
                        // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                        std::string str = "abort\n";
                        memcpy(data_send, str.c_str(), str.length());
                        data_send[str.length()] = '\0';
                        offset = str.length();
                        // 回滚事务
                        txn_manager->abort(context->txn_, log_manager.get());
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
            if (finished_analyze == false) {
                yy_delete_buffer(buf);
            }
            for (int i = 0; i <= strlen(data_send); i++) {
                if (data_send[i] == '\0') {
                    break;
                } else {
                    printf("%c", data_send[i]);
                }
            }
            memset(data_send, 0, 8192);

            // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
            if (context->txn_->get_txn_mode() == false) {
                txn_manager->commit(context->txn_, context->log_mgr_);
            }
        }
    }
}
// bool (*do_cmd)(const std::vector<std::string>&) = my_do_cmd;

bool execute_sql(const std::string& cmd_type, const std::vector<std::string>& tokens) {
    int offset = 0;
    txn_id_t txn_id = INVALID_TXN_ID;
    // 需要返回给客户端的结果
    char data_send[BUFFER_LENGTH];
    auto cmd = fmt::format("{}", fmt::join(tokens, " "));
    if (cmd_type == "exit") {
        stuff.launcher_.shutdown();
        return false;
    } else {
        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;
        auto context = std::make_shared<Context>(
            lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
        SetTransaction(&txn_id, context);
        bool finished_analyze = false;
        YY_BUFFER_STATE buf = yy_scan_string(cmd.c_str());
        if (yyparse() == 0) {
            if (ast::parse_tree != nullptr) {
                try {

                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf);
                    finished_analyze = true;
                    // 优化器
                    std::shared_ptr<Plan> plan =
                        optimizer->plan_query(query, context.get());
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt =
                        portal->start(plan, context.get());
                    portal->run(portalStmt, ql_manager.get(), &txn_id, context.get());
                    portal->drop();
                } catch (TransactionAbortException& exception) {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();
                    // 回滚事务
                    txn_manager->abort(context->txn_, log_manager.get());
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
        if (finished_analyze == false) {
            yy_delete_buffer(buf);
        }
        for (int i = 0; i <= strlen(data_send); i++) {
            if (data_send[i] == '\0') {
                break;
            } else {
                printf("%c", data_send[i]);
            }
        }
        memset(data_send, 0, 8192);

        // 如果是单挑语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        if (context->txn_->get_txn_mode() == false) {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }
    }
}
}; // namespace drmdb_server
using namespace drmdb_server;

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
    set_command_callback(my_do_cmd);
    init_raft(std::make_shared<db_state_machine>());
    loop();

    return 0;
}
