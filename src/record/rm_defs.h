#pragma once
#include "common/errors.h"
#include <boost/crc.hpp>
#include <cstdint>
#include <spdlog/spdlog.h>
constexpr int RM_NO_PAGE = -1;
constexpr int RM_FILE_HDR_PAGE = 0;
constexpr int RM_FIRST_RECORD_PAGE = 1;
constexpr int RM_MAX_RECORD_SIZE = 512;

struct RmFileHdr {
  int record_size; // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
  int num_pages;            // 文件中分配的页面个数（初始化为1）
  int num_records_per_page; // 每个页面最多能存储的元组个数
  int first_free_page_no;   // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
  int bitmap_size;          // 每个页面bitmap大小
};
/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct RmPageHdr {
  int num_records; // 当前页面中当前已经存储的记录个数（初始化为0）
  uint32_t check_sum;
  // 计算页的校验和
  uint32_t calculate_checksum(const char *page_data, std::size_t size) {
    boost::crc_32_type crc32;
    crc32.process_bytes(page_data, size);
    return crc32.checksum();
  }
};

struct RmRecord {
  char *data; // 记录的数据
  int size;
  bool allocated = false;
  uint32_t check_sum;
  RmRecord() = default;
  RmRecord(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated = true;
    check_sum = calculate_checksum();
  };

  RmRecord &operator=(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated = true;
    check_sum = calculate_checksum();
    return *this;
  };

  explicit RmRecord(int size_) {
    size = size_;
    data = new char[size_];
    allocated = true;
    check_sum = calculate_checksum();
  }

  RmRecord(int size_, char *data_) {
    size = size_;
    data = new char[size_];
    memcpy(data, data_, size_);
    allocated = true;
    check_sum = calculate_checksum();
  }

  void SetData(char *data_) {
    memcpy(data, data_, size);
    check_sum = calculate_checksum();
  }

  void Deserialize(const char *data_) {
    size = *reinterpret_cast<const int *>(data_);
    if (allocated) {
      delete[] data;
    }
    data = new char[size];
    memcpy(data, data_ + sizeof(int), size);
    auto deserialize_check_sum = calculate_checksum();
    if (check_sum != deserialize_check_sum) {
      throw InternalError("Checksum failed.");
    }
  }

  ~RmRecord() {
    if (allocated) {
      delete[] data;
    }
    allocated = false;
    data = nullptr;
  }

  char *get_column_value(int offset, int len) {
    char *ret = new char[len];
    memcpy(ret, data + offset, len);
    return ret;
  }
  // 计算记录的校验和
  uint32_t calculate_checksum() const {
    boost::crc_32_type crc32;
    crc32.process_bytes(data, size);
    return crc32.checksum();
  }
  void set_column_value(int offset, int len, char *value) {
    memcpy(data + offset, value, len);
    check_sum = calculate_checksum();
  }
};
