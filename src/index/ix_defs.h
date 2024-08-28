#pragma once
// 无指向页
#include "common/config.h"
#include "common/defs.h"
#include <compare>
#include <tuple>
#include <vector>
constexpr int IX_NO_PAGE = -1;
// 索引文件头页号
constexpr int IX_FILE_HDR_PAGE = 0;
// 索引叶节点头页号
constexpr int IX_LEAF_HEADER_PAGE = 1;
// 索引初始根节点页号
constexpr int IX_INIT_ROOT_PAGE = 2;
// 索引初始页数（文件头+叶节点头+根节点）
constexpr int IX_INIT_NUM_PAGES = 3;
// 索引支持的最长Key（字节）
constexpr int IX_MAX_COL_LEN = 512;

class IxFileHdr {
public:
  page_id_t first_free_page_no_;
  int num_pages_;
  page_id_t root_page_;
  int col_num_;
  std::vector<ColType> col_types_;
  std::vector<int> col_lens_;
  int col_tot_len_;
  int btree_order_;
  int keys_size_;
  page_id_t first_leaf_;
  page_id_t last_leaf_;
  int tot_len_;
  IxFileHdr() { tot_len_ = col_num_ = 0; }
  IxFileHdr(page_id_t first_free_page_no, int num_pages, page_id_t root_page, int col_num, int col_tot_len,
            int btree_order, int keys_size, page_id_t first_leaf, page_id_t last_leaf)
      : first_free_page_no_(first_free_page_no), num_pages_(num_pages), root_page_(root_page), col_num_(col_num),
        col_tot_len_(col_tot_len), btree_order_(btree_order), keys_size_(keys_size), first_leaf_(first_leaf),
        last_leaf_(last_leaf), tot_len_(0) {}
  void update_tot_len() {
    tot_len_ = 0;
    tot_len_ += (sizeof(page_id_t) * 4 + sizeof(int) * 6);
    tot_len_ += (sizeof(ColType) * col_num_ + sizeof(int) * col_num_);
  }
  void serialize(char *dest) {
    int offset = 0;
    memcpy(dest + offset, &tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &first_free_page_no_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &num_pages_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &root_page_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &col_num_, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_types_[i], sizeof(ColType));
      offset += sizeof(ColType);
    }
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_lens_[i], sizeof(int));
      offset += sizeof(int);
    }
    memcpy(dest + offset, &col_tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &btree_order_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &keys_size_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &first_leaf_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &last_leaf_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    assert(offset == tot_len_);
  }

  void deserialize(char *src) {
    int offset = 0;
    tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    first_free_page_no_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(int);
    num_pages_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    root_page_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    col_num_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    for (int i = 0; i < col_num_; ++i) {
      ColType type = *reinterpret_cast<const ColType *>(src + offset);
      offset += sizeof(ColType);
      col_types_.push_back(type);
    }
    for (int i = 0; i < col_num_; ++i) {
      int len = *reinterpret_cast<const int *>(src + offset);
      offset += sizeof(int);
      col_lens_.push_back(len);
    }
    col_tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    btree_order_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    keys_size_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    first_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    last_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    assert(offset == tot_len_);
  }
};

class IxPageHdr {
public:
  page_id_t parent;
  int num_key;
  bool is_leaf;
  page_id_t prev_leaf;
  page_id_t next_leaf;
};

class Iid {
public:
  int page_no;
  int slot_no;

  friend auto operator==(const Iid &x, const Iid &y) -> bool {
    return x.page_no == y.page_no && x.slot_no == y.slot_no;
  }
  friend auto operator<=>(const Iid &x, const Iid &y) -> std::strong_ordering {
    return std::tie(x.page_no, x.slot_no) <=> std::tie(y.page_no, y.slot_no);
  }
  friend auto operator!=(const Iid &x, const Iid &y) -> bool { return !(x == y); }
};
