#pragma once
#include "common/config.h"
#include "storage/disk_manager.h"
#include <boost/unordered/unordered_map_fwd.hpp>
#include <boost/unordered_map.hpp>
#include <common/config.h>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <numeric>
#include <storage/page.h>
#include <utility>
class BufferPoolManager {
public:
  class LRUReplacer {
  private:
    std::mutex latch_;
    std::list<frame_id_t> LRUList_;
    boost::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> LRUHash_;
    size_t max_size_;

  public:
    explicit LRUReplacer(size_t num_pages) : max_size_(num_pages) {}
    ~LRUReplacer() = default;
    auto victim(frame_id_t *frame_id) -> bool {
      std::scoped_lock lock_{latch_};
      if (LRUList_.empty()) {
        *frame_id = INVALID_FRAME_ID;
        return false;
      }
      *frame_id = LRUList_.back();
      LRUList_.pop_back();
      LRUHash_.erase(*frame_id);
      return true;
    }
    void pin(frame_id_t frame_id) {
      std::scoped_lock lock{latch_};
      auto iter = LRUHash_.find(frame_id);
      if (iter != LRUHash_.end()) {
        LRUList_.erase(iter->second);
        LRUHash_.erase(iter);
      }
    }
    void unpin(frame_id_t frame_id) {
      std::scoped_lock<std::mutex> lock{latch_};
      auto iter = LRUHash_.find(frame_id);
      if (iter == LRUHash_.end()) {
        LRUList_.push_front(frame_id);
        LRUHash_.insert(std::make_pair(frame_id, LRUList_.begin()));
      }
    }
    auto size() -> size_t { return LRUList_.size(); }
  };

private:
  size_t pool_size_;
  Page *pages_;
  boost::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
  std::list<frame_id_t> free_list_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<LRUReplacer> replacer_;
  std::mutex latch_;

public:
  BufferPoolManager(size_t pool_size, std::shared_ptr<DiskManager> disk_manager)
      : pool_size_(pool_size), disk_manager_(std::move(disk_manager)) {
    pages_ = new Page[pool_size];
    replacer_ = std::make_shared<LRUReplacer>(pool_size_);
    std::iota(free_list_.begin(), free_list_.end(), 0);
  }
  ~BufferPoolManager() { delete[] pages_; }
  static void mark_dirty(Page *page) { page->is_dirty_ = true; }

public:
  Page *fetch_page(PageId page_id);
  bool unpin_page(PageId page_id, bool is_dirty);
  bool flush_page(PageId page_id);
  Page *new_page(PageId *page_id);
  bool delete_page(PageId page_id);
  void flush_all_pages(int fd);
  void delete_all_pages(int fd);

private:
  bool find_victim_page(frame_id_t *frame_id);
  void update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id);
};
