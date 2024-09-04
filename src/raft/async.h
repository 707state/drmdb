#pragma once
#include "boost/unordered_map.hpp"
#include "ptr.h"
#include "utils.h"
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
namespace raft {
enum cmd_result_code {
    OK = 0,
    CANCELED = -1,
    TIMEOUT = -2,
    NOT_LEADER = -3,
    BAD_REQUEST = -4,
    SERVER_ALREADY_EXISTS = -5,
    CONFIG_CHANGING = -6,
    SERVER_IS_JOINING = -7,
    SERVER_NOT_FOUND = -8,
    CANNOT_REMOVE_LEADER = -9,
    SERVER_IS_LEAVING = -10,
    TERM_MISMATCH = -11,
    RESULT_NOT_EXIST_YET = -1000,
    FAILED = -32768,
};
template <typename T, typename TE = ptr<std::exception>> class cmd_result {
public:
    // 只会被结果触发
    using handler_type = std::function<void(T&, TE&)>;
    // 会被当前实例触发
    using handler_type2 = std::function<void(cmd_result<T, TE>&, TE&)>;
    cmd_result()
        : err_()
        , code_(cmd_result_code::OK)
        , has_result_(false)
        , accepted_(false)
        , handler_(nullptr)
        , handler2_(nullptr) {}

    explicit cmd_result(T& result, cmd_result_code code = cmd_result_code::OK)
        : result_(result)
        , err_()
        , code_(code)
        , has_result_(true)
        , accepted_(false)
        , handler_(nullptr)
        , handler2_(nullptr) {}

    explicit cmd_result(T& result,
                        bool _accepted,
                        cmd_result_code code = cmd_result_code::OK)
        : result_(result)
        , err_()
        , code_(code)
        , has_result_(true)
        , accepted_(_accepted)
        , handler_(nullptr)
        , handler2_(nullptr) {}

    explicit cmd_result(const handler_type& handler)
        : err_()
        , code_(cmd_result_code::OK)
        , has_result_(true)
        , accepted_(false)
        , handler_(handler)
        , handler2_(nullptr) {}

    ~cmd_result() {}

    __nocopy__(cmd_result);

public:
    void reset() {
        std::lock_guard guard{lock_};
        err_ = TE();
        code_ = cmd_result_code::OK;
        has_result_ = false;
        accepted_ = false;
        handler_ = nullptr;
        handler2_ = nullptr;
        result_ = T();
    }
    // 当复制的结果被获取到时触发的handler
    /*
     * @param handler_type handler
     * @ return void
     */
    void when_ready(const handler_type& handler) {
        bool call_handler = false;
        {
            std::lock_guard guard{lock_};
            if (has_result_) {
                call_handler = true;
            } else {
                handler_ = handler;
            }
        }
        if (call_handler) handler(result_, err_);
    }
    void when_ready(const handler_type2& hadnler) {
        bool call_handler = false;
        {
            std::lock_guard guard{lock_};
            if (has_result_)
                call_handler = true;
            else
                handler2_ = hadnler;
        }
        if (call_handler) hadnler(*this, err_);
    }
    void set_result(T& result, TE& err, cmd_result_code code = cmd_result_code::OK) {
        bool call_handler = false;
        {
            std::lock_guard<std::mutex> guard{lock_};
            result_ = result;
            err_ = err;
            has_result_ = true;
            if (handler_ || handler2_) call_handler = true;
        }
        if (call_handler) {
            if (handler2_)
                handler2_(*this, err);
            else if (handler_)
                handler_(result, err);
        }
        cv_.notify_all();
    }
    void accept() { accepted_ = true; }
    bool get_accepted() const { return accepted_; }
    void set_result_code(cmd_result_code code) {
        std::lock_guard guard{lock_};
        code_ = code;
    }
    cmd_result_code get_result_code() const {
        std::lock_guard guard{lock_};
        if (has_result_) {
            return code_;
        } else {
            return RESULT_NOT_EXIST_YET;
        }
    }
    bool has_result() const {
        std::lock_guard guard{lock_};
        return has_result_;
    }

    std::string get_result_str() const {
        cmd_result_code code = get_result_code();

        static boost::unordered_map<int, std::string> code_str_map(
            {{cmd_result_code::OK, "Ok."},
             {cmd_result_code::CANCELED, "Request cancelled."},
             {cmd_result_code::TIMEOUT, "Request timeout."},
             {cmd_result_code::NOT_LEADER, "This node is not a leader."},
             {cmd_result_code::BAD_REQUEST, "Invalid request."},
             {cmd_result_code::SERVER_ALREADY_EXISTS,
              "Server already exists in the cluster."},
             {cmd_result_code::CONFIG_CHANGING,
              "Previous configuration change has not been committed yet."},
             {cmd_result_code::SERVER_IS_JOINING, "Other server is being added."},
             {cmd_result_code::SERVER_NOT_FOUND, "Cannot find server."},
             {cmd_result_code::CANNOT_REMOVE_LEADER, "Cannot remove leader."},
             {cmd_result_code::TERM_MISMATCH,
              "The current term does not match the expected term."},
             {cmd_result_code::RESULT_NOT_EXIST_YET,
              "Operation is in progress and the result does not exist yet."},
             {cmd_result_code::FAILED, "Failed."}});
        auto entry = code_str_map.find(static_cast<int>(code));
        if (entry == code_str_map.end()) {
            return "Unknown (" + std::to_string(static_cast<int>(code)) + ").";
        }
        return entry->second;
    }
    T& get() {
        std::unique_lock guard{lock_};
        if (has_result_) {
            if (err_ == nullptr) {
                return result_;
            }
            return empty_result;
        }
        cv_.wait(guard);
        if (err_ == nullptr) {
            return result_;
        }
        return empty_result;
    }

private:
    T empty_result;
    T result_;
    TE err_;
    cmd_result_code code_;
    bool has_result_;
    bool accepted_;
    handler_type handler_;
    handler_type2 handler2_;
    mutable std::mutex lock_;
    std::condition_variable cv_;
};
template <typename T, typename TE = ptr<std::exception>>
using async_result = cmd_result<T, TE>;
} // namespace raft
