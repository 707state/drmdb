#pragma once
#include "logger.h"
#include "raft/logger.hxx"
using namespace nuraft;

/**
 * Example implementation of Raft logger, on top of SimpleLogger.
 */
class logger_wrapper : public logger {
public:
    logger_wrapper(const std::string& log_file, int log_level = 6) {
        my_log_ = new SimpleLogger(log_file, 1024, 32 * 1024 * 1024, 10);
        my_log_->setLogLevel(log_level);
        my_log_->setDispLevel(-1);
        my_log_->setCrashDumpPath("./", true);
        my_log_->start();
    }

    ~logger_wrapper() { destroy(); }

    void destroy() {
        if (my_log_) {
            my_log_->flushAll();
            my_log_->stop();
            delete my_log_;
            my_log_ = nullptr;
        }
    }

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& msg) {
        if (my_log_) {
            my_log_->put(level, source_file, func_name, line_number, "%s", msg.c_str());
        }
    }

    void set_level(int l) {
        if (!my_log_) return;

        if (l < 0) l = 1;
        if (l > 6) l = 6;
        my_log_->setLogLevel(l);
    }

    int get_level() {
        if (!my_log_) return 0;
        return my_log_->getLogLevel();
    }

    SimpleLogger* getLogger() const { return my_log_; }

private:
    SimpleLogger* my_log_;
};
