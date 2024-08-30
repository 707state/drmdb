#pragma once
#include "raft/utils.h"
#include <cstddef>
#include <string>
namespace raft {
class logger {
  __interface_body(logger);

public:
  virtual void debug(const std::string &log_line) {}
  virtual void info(const std::string &log_line) {}
  virtual void warn(const std::string &log_line) {}
  virtual void err(const std::string &log_line) {}
  virtual void set_level(int l) {}
  virtual int get_level() { return 6; }
  virtual void put_detils(int level, const char *source_file, const char *func_name, size_t line_number,
                          const std::string &log_line) {}
};
} // namespace raft
