#pragma once
#include "common/config.h"
#include <boost/atomic.hpp>
#include <boost/unordered/unordered_map_fwd.hpp>
#include <boost/unordered_map.hpp>
#include <cstdint>
#include <string>
#include <string_view>
class DiskManager {
private:
  static constexpr uint32_t MAX_FD = 8192;
  boost::unordered_map<std::string_view, int> path_2_fd_;
  boost::unordered_map<int, std::string_view> fd_2_path_;
  int log_fd_ = -1;
  boost::atomic<int> fd_2_page_num_[MAX_FD]{};

public:
  DiskManager();
  ~DiskManager() = default;
  // Page operation
  void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);
  void write_full_pages(int fd, page_id_t page_no, const char *offset, int page_count);
  void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);
  page_id_t allocate_page(int fd);
  void deallocate_page(page_id_t page_id);
  // directory operation
  bool is_dir(const std::string_view &path);
  void create_dir(const std::string &path);
  void destroy_dir(const std::string &path);
  // file operation
  bool is_file(const std::string_view &path);
  void create_file(const std::string &path);
  void destroy_file(const std::string &path);
  int open_file(const std::string &path);
  void close_file(int fd);
  int get_file_size(const std::string &path);
  std::string_view get_file_name(int fd);
  auto get_file_fd(const std::string &path) -> int;
  // log operation
  int read_log(char *log_data, int size, int offset);
  void write_log(char *log_data, int size);
  void set_log_fd(int log_fd) { this->log_fd_ = log_fd; }
  int get_log_fg() { return log_fd_; }
  // 文件:->文件page数目的映射
  void set_fd_2_page_no(int fd, int start_page_no) { fd_2_page_num_[fd] = start_page_no; }
  page_id_t get_fd_2_page_no(int fd) { return fd_2_page_num_[fd]; }
  // TODO: 添加一个JSON格式的日志
};
