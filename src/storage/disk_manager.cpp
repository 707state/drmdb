#include "common/config.h"
#include "common/errors.h"
#include <boost/filesystem.hpp>
#include <cstdio>
#include <storage/disk_manager.h>
#include <string_view>
#include <unistd.h>

DiskManager::DiskManager() { memset(fd_2_page_num_, 0, MAX_FD * (sizeof(boost::atomic<page_id_t>) / sizeof(char))); }
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
  auto file_offset = page_no * PAGE_SIZE;
  lseek(fd, static_cast<off_t>(file_offset), SEEK_SET);
  auto written_bytes = write(fd, offset, num_bytes);
  if (num_bytes != written_bytes) {
    throw InternalError("DiskManager::write_page error");
  }
}
void DiskManager::write_full_pages(int fd, page_id_t page_no, const char *offset, int page_count) {
  auto file_offset = page_no * PAGE_SIZE;
  lseek(fd, static_cast<off_t>(file_offset), SEEK_SET);
  write(fd, offset, page_count * PAGE_SIZE);
}
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
  auto file_offset = page_no * PAGE_SIZE;
  lseek(fd, static_cast<off_t>(file_offset), SEEK_SET);
  auto read_bytes = read(fd, offset, num_bytes);
  if (read_bytes != num_bytes) {
    throw InternalError("DiskManager::read_page Error");
  }
}
page_id_t DiskManager::allocate_page(int fd) {
  // 简单的自增分配策略，指定文件的页面编号加1
  assert(fd >= 0 && fd < MAX_FD);
  return fd_2_page_num_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string_view &path) {
  struct stat st {};
  return stat(path.data(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
  std::string cmd = "mkdir " + path;
  if (system(cmd.c_str()) < 0) {
    throw UnixError();
  }
}

void DiskManager::destroy_dir(const std::string &path) {
  std::string cmd = "rm -r " + path;
  if (system(cmd.c_str()) < 0) {
    throw UnixError();
  }
}
bool DiskManager::is_file(const std::string_view &path) {
  struct stat st {};
  return stat(path.data(), &st) == 0 && S_ISREG(st.st_mode);
}

void DiskManager::create_file(const std::string &path) {
  if (is_file(path))
    throw FileExistsError(path);
  int fd = open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
  if (fd < 0)
    throw UnixError();
  close(fd);
}
void DiskManager::destroy_file(const std::string &path) {
  if (!is_file(path))
    throw FileNotFoundError(path);
  if (path_2_fd_.count(path))
    throw FileNotClosedError(path);
  auto &&unlink_result = unlink(path.c_str());
  if (unlink_result != 0)
    throw UnixError();
}

auto DiskManager::open_file(const std::string &path) -> int {
  if (!is_file(path))
    throw FileNotFoundError(path);
  if (path_2_fd_.count(path))
    throw FileNotClosedError(path);
  int fd = open(path.c_str(), O_RDWR);
  path_2_fd_[path] = fd;
  fd_2_path_[fd] = path;
  return fd;
}

void DiskManager::close_file(int fd) {
  if (!fd_2_path_.count(fd))
    throw FileNotOpenError(fd);
  close(fd);
  path_2_fd_.erase(fd_2_path_[fd]);
  fd_2_path_.erase(fd);
}
auto DiskManager::get_file_size(const std::string &file_name) -> int {
  struct stat stat_buf {};
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}
auto DiskManager::get_file_name(int fd) -> std::string_view {
  if (!fd_2_path_.count(fd))
    throw FileNotOpenError(fd);
  return fd_2_path_[fd];
}
auto DiskManager::get_file_fd(const std::string &file_name) -> int {
  if (!path_2_fd_.count(file_name))
    return open_file(file_name);
  return path_2_fd_[file_name];
}
auto DiskManager::read_log(char *log_data, int size, int offset) -> int {
  if (log_fd_ == -1) {
    log_fd_ = open_file(LOG_FILE_NAME);
  }
  int file_size = get_file_size(LOG_FILE_NAME);
  if (offset > file_size) {
    return -1;
  }
  size = std::min(size, file_size - offset);
  if (size == 0) {
    return 0;
  }
  lseek(log_fd_, offset, SEEK_SET);
  ssize_t bytes_read = read(log_fd_, log_data, size);
  assert(bytes_read == size);
  return bytes_read;
}
void DiskManager::write_log(char *log_data, int size) {
  if (log_fd_ == -1) {
    log_fd_ = open_file(LOG_FILE_NAME);
  }

  // write from the file_end
  lseek(log_fd_, 0, SEEK_END);
  ssize_t bytes_write = write(log_fd_, log_data, size);
  if (bytes_write != size) {
    printf("Error in file: %s, line: %d\n", __FILE__, __LINE__);

    throw UnixError();
  }
}
