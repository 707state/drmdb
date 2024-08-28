#include <storage/buffer_pool_manager.h>
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  } else {
    if (!replacer_->victim(frame_id)) {
      return false;
    }
    Page *page = &pages_[*frame_id];

    if (page->is_dirty_) {
      disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->get_data(), PAGE_SIZE);
      page->is_dirty_ = false;
    }
    return true;
  }
}

void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
  if (page->is_dirty_) {
    disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->get_data(), PAGE_SIZE);
  }
  page_table_.erase(page->id_);
  page_table_[new_page_id] = new_frame_id;
  page->reset_memory();
  page->id_ = new_page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
}
Page *BufferPoolManager::fetch_page(PageId page_id) {
  std::scoped_lock lock{latch_};
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    Page *page = &pages_[it->second];
    page->pin_count_ += 1;
    if (page->pin_count_ == 1) {
      replacer_->pin(it->second);
    }
    return page;
  } else {
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
      return nullptr;
    }
    Page *page = &pages_[frame_id];
    update_page(page, page_id, frame_id);
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->get_data(), PAGE_SIZE);
    page->pin_count_ += 1;
    if (page->pin_count_ == 1) {
      replacer_->pin(frame_id);
    }
    return page;
  }
}

bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  } else {
    int pin_count = pages_[it->second].pin_count_;
    if (pin_count == 0) {
      return false;
    } else {
      pages_[it->second].pin_count_ -= 1;
      if (pages_[it->second].pin_count_ == 0) {
        replacer_->unpin(it->second);
      }
      if (is_dirty) {
        pages_[it->second].is_dirty_ = true;
      }
    }
    return true;
  }
}
bool BufferPoolManager::flush_page(PageId page_id) {
  std::scoped_lock lock{latch_};
  assert(page_id.page_no != INVALID_PAGE_ID);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  Page *page = &pages_[it->second];
  disk_manager_->write_page(page_id.fd, page_id.page_no, page->get_data(), PAGE_SIZE);
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::new_page(PageId *page_id) {
  std::scoped_lock lock{latch_};
  frame_id_t frame_id;
  if (!find_victim_page(&frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  *page_id = {page_id->fd, disk_manager_->allocate_page(page_id->fd)};
  update_page(page, *page_id, frame_id);
  page->pin_count_ += 1;
  if (page->pin_count_ == 1) {
    replacer_->pin(frame_id);
  }
  return page;
}
bool BufferPoolManager::delete_page(PageId page_id) {
  std::scoped_lock lock{latch_};
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  if (pages_[it->second].pin_count_ != 0) {
    return false;
  }
  if (pages_[it->second].is_dirty_) {
    disk_manager_->write_page(page_id.fd, page_id.page_no, pages_[it->second].get_data(), PAGE_SIZE);
    pages_[it->second].is_dirty_ = false;
  }
  page_table_.erase(it);
  pages_[it->second].reset_memory();
  free_list_.push_back(it->second);
  pages_[it->second].pin_count_ = 0;
  return true;
}
void BufferPoolManager::flush_all_pages(int fd) {
  std::scoped_lock lock{latch_};
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it) {
    if (it->first.fd == fd) {
      Page *page = &pages_[it->second];
      disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->get_data(), PAGE_SIZE);
      page->is_dirty_ = false;
    }
  }
}
void BufferPoolManager::delete_all_pages(int fd) {
  std::scoped_lock lock{latch_};
  std::vector<PageId> to_be_deleted;
  for (auto &[page_id, frame] : page_table_) {
    if (page_id.fd == fd) {
      to_be_deleted.push_back(page_id);
    }
  }
  for (auto page_id : to_be_deleted) {
    auto frame = page_table_[page_id];
    // 如果该页面还没有unpin，则unpin
    replacer_->unpin(frame);
    // 从页表中删除该页面并添加到free_list中
    Page *page = &(pages_[frame]);
    page->reset_memory();
    page->is_dirty_ = false;
    page->pin_count_ = 0;

    page_table_.erase(page_id);
    free_list_.emplace_back(frame);
  }
}
