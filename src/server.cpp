#include "index/ix_manager.h"
#include "raft/buffer_serializer.hxx"
#include "raft/ptr.hxx"
#include "raft/raft_params.hxx"
#include "record/rm_manager.h"
#include "server/server_operation.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"
#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/lexical_cast.hpp>
#include <cassert>
#include <exception>
#include <execution/execution_manager.h>
#include <functional>
#include <optimizer/optimizer.h>
#include <raft/nuraft.hxx>
#include <record/rm_manager.h>
#include <recovery/log_manager.h>
#include <recovery/log_recovery.h>
#include <server/portal.h>
#include <string>
#include <vector>
using namespace nuraft;
using boost::asio::steady_timer;
namespace drmdb_server {
// 构建全局所需的管理器对象
auto disk_manager = std::make_shared<DiskManager>();
auto buffer_pool_manager =
    std::make_shared<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager =
    std::make_shared<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager =
    std::make_shared<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager = std::make_shared<SmManager>(
    disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_shared<LockManager>();
auto txn_manager =
    std::make_shared<TransactionManager>(lock_manager.get(), sm_manager.get());
auto ql_manager = std::make_shared<QlManager>(sm_manager.get(), txn_manager.get());
auto log_manager = std::make_shared<LogManager>(disk_manager.get());
auto recovery = std::make_shared<RecoveryManager>(
    disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
auto planner = std::make_shared<Planner>(sm_manager.get());
auto optimizer = std::make_shared<Optimizer>(sm_manager.get(), planner.get());
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

    ptr<steady_timer> timer = new_ptr<steady_timer>();
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
bool do_cmd(const std::vector<std::string>& tokens) {
    const auto& cmd_type = tokens[0];
    if (cmd_type == "st" || cmd_type == "stat") {
        print_status(cmd_type, tokens);
    }
}

}; // namespace drmdb_server
using namespace drmdb_server;
