#pragma once
#include "common/config.h"
#include "common/defs.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "storage/page.h"
#include "transaction/transaction.h"

#include <cassert>
#include <common/errors.h>
#include <cstddef>
#include <cstring>
#include <index/ix_defs.h>
#include <math.h>
#include <memory>
#include <mutex>
#include <record/rm_defs.h>
#include <system/sm_meta.h>
#include <utility>
#include <vector>
enum class Operation { FIND = 0, INSERT, DELETE };
static constexpr bool binary_search = false;

inline auto ix_compare(const char *a, const char *b, ColType type, int col_len) -> int {
  switch (type) {
  case TYPE_INT: {
    int ia = *(int *)a;
    //      if (ia == INT32_MIN || ia == INT32_MAX) return 0;
    int ib = *(int *)b;
    return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
  }
  case TYPE_FLOAT: {
    float fa = *(float *)a;
    //      if (fa == __FLT_MIN__ || fa == __FLT_MAX__) return 0;
    float fb = *(float *)b;
    return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
  }
  case TYPE_STRING: {
    return memcmp(a, b, col_len);
  }
  case TYPE_DATETIME: {
    // type datetime以uint64位形式存储
    uint64_t date_a = *(uint64_t *)a;
    uint64_t date_b = *(uint64_t *)b;
    return (date_a < date_b) ? -1 : ((date_a > date_b) ? 1 : 0);
  }
  default: {
    throw InternalError("Unexpected data type");
  }
  }
}

inline auto ix_compare(const char *a, const char *b, const std::vector<ColType> &col_types,
                       const std::vector<int> &col_lens) -> int {
  int offset = 0;
  for (size_t i = 0; i < col_types.size(); ++i) {
    int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
    if (res != 0)
      return res;
    offset += col_lens[i];
  }
  return 0;
}

inline auto ix_make_key(char *dst, char *raw_record, IndexMeta &index_meta) -> void {
  int offset = 0;
  for (size_t i = 0; i < index_meta.col_num; i++) {
    std::memcpy(dst + offset, raw_record + index_meta.cols[i].offset, index_meta.cols[i].len);
    offset += index_meta.cols[i].len;
  }
}

inline auto ix_make_key(char *dst, RmRecord record, IndexMeta &index_meta) -> void {
  int offset = 0;
  for (size_t i = 0; i < index_meta.col_num; i++) {
    std::memcpy(dst + offset, record.data + index_meta.cols[i].offset, index_meta.cols[i].len);
    offset += index_meta.cols[i].len;
  }
}
inline auto key_to_string(const char *key, const std::vector<ColType> &col_types,
                          const std::vector<int> &col_lens) -> std::string {
  std::string key_str = "|";
  int val_int = 0;
  float val_flt = NAN;
  uint64_t val_datetime = 0;
  int offset = 0;
  for (int i = 0; i < col_types.size(); ++i) {
    auto type = col_types[i];
    switch (type) {
    case TYPE_INT:
      std::memcpy(&val_int, key + offset, col_lens[i]);
      key_str += std::to_string(val_int);
      key_str += "|";
      break;
    case TYPE_FLOAT:
      std::memcpy(&val_flt, key + offset, col_lens[i]);
      key_str += std::to_string(val_flt);
      key_str += "|";
      break;
    case TYPE_STRING:
      key_str.append(key + offset, col_lens[i]);
      key_str += "|";
      break;
    case TYPE_DATETIME:
      std::memcpy(&val_datetime, key + offset, col_lens[i]);
      DateTime date_time{};
      date_time.decode(val_datetime);
      key_str += date_time.encode_to_string();
      key_str += "|";
      break;
    }
    offset += col_lens[i];
  }
  return key_str;
}
class IxNodeHandle {
  friend class IxIndexHandle;
  friend class IxScan;

private:
  const IxFileHdr *file_hdr;
  Page *page;
  IxPageHdr *page_hdr;
  char *keys;
  Rid *rids;

public:
  IxNodeHandle() = default;
  IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_)
      : file_hdr(file_hdr_), page(page_), page_hdr(reinterpret_cast<IxPageHdr *>(page->get_data())),
        keys(page->get_data() + sizeof(IxPageHdr)), rids(reinterpret_cast<Rid *>(keys + file_hdr->keys_size_)) {}
  auto get_size() -> int { return page_hdr->num_key; }
  void set_size(int size) { page_hdr->num_key = size; }
  auto get_max_size() -> int { return file_hdr->btree_order_ + 1; }
  auto get_min_size() -> int { return get_max_size() / 2; }
  auto value_at(int i) -> page_id_t { return get_rid(i)->page_no; }
  auto get_page_no() -> page_id_t { return page->get_page_id().page_no; }
  auto get_page_id() -> PageId { return page->get_page_id(); }
  auto get_next_leaf() -> page_id_t { return page_hdr->next_leaf; }
  auto get_prev_leaf() -> page_id_t { return page_hdr->prev_leaf; }
  auto get_parent_page_no() -> page_id_t { return page_hdr->parent; }
  auto is_leaf_page() -> bool { return page_hdr->is_leaf; }
  auto is_root_page() -> bool { return get_parent_page_no() == INVALID_PAGE_ID; }
  void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }
  void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }
  void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }
  [[nodiscard]] auto get_key(int key_idx) const -> char * {
    return keys + static_cast<ptrdiff_t>(key_idx * file_hdr->col_tot_len_);
  }
  Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }
  auto lower_bound(const char *target) const -> int;
  auto upper_bound(const char *target) const -> int;
  void insert_pairs(int pos, const char *key, const Rid *rid, int n);
  auto internal_lookup(const char *key) -> page_id_t;
  auto leaf_lookup(const char *key, Rid **values) -> bool;
  auto insert(const char *key, const Rid &value) -> int;
  void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }
  void erase_pair(int pos);
  auto remove(const char *key) -> int;
  auto find_child(IxNodeHandle *child) -> int {
    int rid_idx = 0;
    for (; rid_idx < page_hdr->num_key; rid_idx++) {
      if (get_rid(rid_idx)->page_no == child->get_page_no()) {
        break;
      }
    }
    assert(rid_idx < page_hdr->num_key);
    return rid_idx;
  }
  auto exists_key(const char *key) const -> bool {
    int key_num = page_hdr->num_key;
    for (int i = 0; i < key_num; i++) {
      char *key_addr = get_key(i);
      if (ix_compare(key, key_addr, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return true;
      }
    }
    return false;
  }
};
class IxIndexHandle {
  friend class IxScan;
  friend class IxManager;

private:
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;
  int fd_;
  IxFileHdr *file_hdr_;
  std::mutex root_latch_;

public:
  IxIndexHandle(std::shared_ptr<DiskManager> disk_manager, std::shared_ptr<BufferPoolManager> buffer_pool_manager,
                int fd);
  ~IxIndexHandle();
  auto get_value(const char *key, std::vector<Rid> *result, Transaction *txn) -> bool;
  auto find_leaf_page(const char *page, Operation operation, Transaction *transaction,
                      bool find_first = false) -> std::pair<IxNodeHandle *, bool>;
  auto insert_entry(const char *ken, const Rid &value, Transaction *transaction) -> page_id_t;
  auto split(IxNodeHandle *node) -> IxNodeHandle *;
  void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);
  bool delete_entry(const char *key, Transaction *transaction);
  bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);
  auto adjust_root(IxNodeHandle *old_root_node) -> bool;
  void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);
  auto coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                Transaction *transaction, bool *root_is_latched) -> bool;
  auto lower_bound(const char *key) -> Iid;
  auto upper_bound(const char *key) -> Iid;
  Iid leaf_end();
  Iid leaf_begin();
  int get_fd() { return fd_; }
  void rebuild_index_from_load(char *key, Rid value);
  void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }
  bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }
  IxNodeHandle *fetch_node(int page_no) const;
  IxNodeHandle *create_node();
  void release_node(IxNodeHandle *node_handle, bool dirty);
  void maintain_parent(IxNodeHandle *node);
  void erase_leaf(IxNodeHandle *leaf);
  void release_node_handle(IxNodeHandle &node);
  void maintain_child(IxNodeHandle *node, int child_idx);
  Rid get_rid(const Iid &iid) const;
  auto print_node_key(IxNodeHandle *node) -> void {
    std::cout << key_to_string(node->get_key(0), file_hdr_->col_types_, file_hdr_->col_lens_) << " ... ";
    std::cout << key_to_string(node->get_key(node->get_size() - 1), file_hdr_->col_types_, file_hdr_->col_lens_) << " ";
  }

  auto print_sub_tree(IxNodeHandle *cur_node) -> void {
    print_node_key(cur_node);
    std::cout << std::endl;
    if (cur_node->is_leaf_page()) {
      return;
    } else {
      for (int i = 0; i < cur_node->get_size(); ++i) {
        std::cout << cur_node->get_rid(i)->page_no << "-";
        auto sub_tree_root = fetch_node(cur_node->get_rid(i)->page_no);
        print_node_key(sub_tree_root);
        release_node(sub_tree_root, false);
      }
      std::cout << std::endl;
    }
  }

  auto print_total_tree() -> void {
    auto root_node = fetch_node(file_hdr_->root_page_);
    print_sub_tree(root_node);
    release_node(root_node, false);
  }
};
