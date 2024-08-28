#pragma once
#include "common/config.h"
#include <boost/container_hash/hash.hpp>
#include <boost/format.hpp>
#include <boost/format/format_fwd.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
struct PageId {
  int fd;                              // Page对应的文件的Fd
  page_id_t page_no = INVALID_PAGE_ID; // 文件中的页号

  friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; }

  bool operator<(const PageId &x) const {
    if (fd < x.fd) {
      return true;
    }
    return page_no < x.page_no;
  }
  bool operator>(const PageId &x) const {
    if (fd > x.fd) {
      return true;
    }
    return page_no > x.page_no;
  }

  std::string toString() { return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}"; }

  inline int64_t Get() const { return (static_cast<int64_t>(fd << 16) | page_no); }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
  size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

template <> struct std::hash<PageId> {
  size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};
template <> struct boost::hash<PageId> {
  size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

class Page {
  friend class BufferPoolManager;

public:
  Page() { reset_memory(); }

  ~Page() = default;

  PageId get_page_id() const { return id_; }

  inline char *get_data() { return data_; }

  bool is_dirty() const { return is_dirty_; }

  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 0;

  inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN); }

  inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

private:
  // 重置Page中的数据
  void reset_memory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  PageId id_;

  char data_[PAGE_SIZE] = {};

  bool is_dirty_ = false;

  int pin_count_ = 0;
};
