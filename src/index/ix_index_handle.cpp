#include "common/config.h"
#include "transaction/transaction.h"
#include <cstddef>
#include <cstring>
#include <index/ix_index_handle.h>
#include <stdexcept>
#include <utility>
auto IxNodeHandle::lower_bound(const char *target) const -> int {
  if (binary_search) {
    int left = 0, right = page_hdr->num_key;
    while (left < right) {
      int mid = left + (right - left) / 2;
      char *key_addr = get_key(mid);
      if (ix_compare(target, key_addr, file_hdr->col_types_, file_hdr->col_lens_) <= 0) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return left;
  } else {
    // 顺序查找
    int key_index = 0;
    for (; key_index < page_hdr->num_key; key_index++) {
      char *key_addr = get_key(key_index);
      if (ix_compare(target, key_addr, file_hdr->col_types_, file_hdr->col_lens_) <= 0) {
        break;
      }
    }
    return key_index;
  }
}
auto IxNodeHandle::upper_bound(const char *target) const -> int {
  if (binary_search) {
    int left = 0, right = page_hdr->num_key;
    while (left < right) {
      int mid = left + (right - mid) / 2;
      char *key_addr = get_key(mid);
      if (ix_compare(target, key_addr, file_hdr->col_types_, file_hdr->col_lens_) < 0) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return left;
  } else {
    int key_index = 1;
    for (; key_index < page_hdr->num_key; key_index++) {
      char *key_addr = get_key(key_index);
      if (ix_compare(target, key_addr, file_hdr->col_types_, file_hdr->col_lens_) < 0) {
        break;
      }
    }
    return key_index;
  }
}
auto IxNodeHandle::leaf_lookup(const char *key, Rid **value) -> bool {
  int idx = lower_bound(key);
  if (idx == page_hdr->num_key ||
      ix_compare(key, keys + idx * file_hdr->col_tot_len_, file_hdr->col_types_, file_hdr->col_lens_) != 0) {
    return false;
  }
  *value = get_rid(idx);
  return true;
}
auto IxNodeHandle::internal_lookup(const char *key) -> page_id_t {
  int pos = upper_bound(key);
  pos--;
  return value_at(pos);
}
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
  if (pos < 0 || pos > page_hdr->num_key) [[unlikely]] {
    throw std::runtime_error("pos is illegal");
  }
  char *key_slot = keys + static_cast<ptrdiff_t>(pos * file_hdr->col_tot_len_);
  std::memmove(key_slot + static_cast<ptrdiff_t>(n * file_hdr->col_tot_len_), key_slot,
               static_cast<size_t>((page_hdr->num_key - pos) * file_hdr->col_tot_len_));
  std::memcpy(key_slot, key, static_cast<size_t>(n * file_hdr->col_tot_len_));
  char *rid_slot = keys + static_cast<ptrdiff_t>(pos * file_hdr->col_tot_len_);
  memmove(rid_slot + n * sizeof(Rid), rid_slot, (page_hdr->num_key - pos) * sizeof(Rid));
  memcpy(rid_slot, rid, n * sizeof(Rid));
  page_hdr->num_key += n;
}
int IxNodeHandle::insert(const char *key, const Rid &value) {
  int pos = lower_bound(key);
  if (pos < page_hdr->num_key && ix_compare(key, keys + static_cast<ptrdiff_t>(pos * file_hdr->col_tot_len_),
                                            file_hdr->col_types_, file_hdr->col_lens_) == 0) {
    return page_hdr->num_key;
  }
  insert_pairs(pos, key, &value, 1);
  return page_hdr->num_key;
}
void IxNodeHandle::erase_pair(int pos) {
  // 判断pos合法性
  if (pos < 0 || pos >= page_hdr->num_key) {
    throw std::runtime_error("pos is illegal");
  }
  // 删除该位置的key
  char *key_slot = keys + static_cast<ptrdiff_t>(pos * file_hdr->col_tot_len_);
  memmove(key_slot, key_slot + file_hdr->col_tot_len_,
          static_cast<size_t>((page_hdr->num_key - pos - 1) * file_hdr->col_tot_len_));
  // 删除该位置的rid
  char *rid_slot = reinterpret_cast<char *>(rids) + pos * sizeof(Rid);
  memmove(rid_slot, rid_slot + sizeof(Rid), (page_hdr->num_key - pos - 1) * sizeof(Rid));
  // 更新结点的键值对数量
  page_hdr->num_key--;
}
auto IxNodeHandle::remove(const char *key) -> int {
  // 查找要删除键值对的位置
  int pos = lower_bound(key);
  // 如果要删除的键值对存在，删除
  if (pos != page_hdr->num_key && ix_compare(key, keys + static_cast<ptrdiff_t>(pos * file_hdr->col_tot_len_),
                                             file_hdr->col_types_, file_hdr->col_lens_) == 0) {
    erase_pair(pos);
  }
  // 返回完成删除操作后的键值对数量
  return page_hdr->num_key;
}
IxIndexHandle::IxIndexHandle(std::shared_ptr<DiskManager> disk_manager,
                             std::shared_ptr<BufferPoolManager> buffer_pool_manager, int fd)
    : disk_manager_(std::move(disk_manager)), buffer_pool_manager_(std::move(buffer_pool_manager)), fd_(fd),
      file_hdr_(new IxFileHdr()) {
  // 读取索引文件头
  char buf[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
  // 反序列化

  file_hdr_->deserialize(buf);
  // 设置页数，用于后续增加新页
  disk_manager_->set_fd_2_page_no(fd, file_hdr_->num_pages_);
}

IxIndexHandle::~IxIndexHandle() { delete file_hdr_; }

std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                              Transaction *transaction, bool find_first) {
  auto cur_node = fetch_node(file_hdr_->root_page_);
  while (!cur_node->is_leaf_page()) {
    auto next_node_page_no = cur_node->internal_lookup(key);
    release_node(cur_node, false);
    cur_node = fetch_node(next_node_page_no);
  }
  return std::make_pair(cur_node, find_first);
}
auto IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) -> bool {
  std::scoped_lock lock{root_latch_};
  // 获取目标key值所在的叶子结点
  auto leaf_pair = find_leaf_page(key, Operation::FIND, transaction);
  auto leaf_node = leaf_pair.first;
  if (leaf_node == nullptr) {
    return false;
  }

  // 在叶子节点中查找目标key值的位置，并读取key对应的rid
  Rid *rid = nullptr;

  bool res = leaf_node->leaf_lookup(key, &rid);
  if (res) {
    // 把rid存入result参数中
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    result->push_back(*rid);
    delete leaf_node;
    return true;
  }

  return false;
}
auto IxIndexHandle::split(IxNodeHandle *node) -> IxNodeHandle * {
  // 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
  IxNodeHandle *new_node = create_node();
  // 需要初始化新节点的page_hdr内容
  new_node->page_hdr->num_key = 0;
  new_node->page_hdr->is_leaf = node->page_hdr->is_leaf;
  new_node->page_hdr->parent = node->get_parent_page_no();
  // 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
  if (new_node->is_leaf_page()) {
    new_node->page_hdr->prev_leaf = node->get_page_no();
    new_node->page_hdr->next_leaf = node->get_next_leaf();
    node->page_hdr->next_leaf = new_node->get_page_no();

    // 为新节点分配键值对，更新旧节点的键值对数记录
    // 如果新的右兄弟节点不是叶子结点，就获取新节点的后继节点
    IxNodeHandle *next_node = fetch_node(new_node->page_hdr->next_leaf);
    // 更新新节点的后继节点
    next_node->page_hdr->prev_leaf = new_node->get_page_no();
    release_node(next_node, true);
  }

  // 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
  // 将原节点的一部分键值对移动到新节点中，平均分配
  int pos = node->page_hdr->num_key / 2;
  int num = node->get_size() - pos;
  new_node->insert_pairs(0, node->get_key(pos), node->get_rid(pos), num);
  node->page_hdr->num_key = pos;
  for (int i = 0; i < num; i++) {
    maintain_child(new_node, i);
  }
  return new_node;
}
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                       Transaction *transaction) {
  if (old_node->is_root_page()) {
    // 根结点需要分配新的root
    auto *new_root = create_node();
    new_root->page_hdr->is_leaf = false;
    new_root->set_size(0);
    new_root->set_parent_page_no(INVALID_PAGE_ID);
    new_root->insert_pair(0, old_node->get_key(0), {old_node->get_page_no(), -1});
    new_root->insert_pair(1, key, {new_node->get_page_no(), -1});
    int new_root_page_no = new_root->get_page_no();
    file_hdr_->root_page_ = new_root_page_no;
    old_node->set_parent_page_no(new_root_page_no);
    new_node->set_parent_page_no(new_root_page_no);
    release_node(new_root, true);
  } else {
    auto parent = fetch_node(old_node->get_parent_page_no());
    int pos = parent->find_child(old_node);
    parent->insert_pair(pos + 1, key, {new_node->get_page_no(), -1});
    new_node->set_parent_page_no(parent->get_page_no());
    if (parent->get_size() >= parent->get_max_size()) {
      auto new_parent = split(parent);
      insert_into_parent(parent, new_parent->get_key(0), new_parent, transaction);
      release_node(new_parent, true);
    }
    release_node(parent, true);
  }
}
auto IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) -> page_id_t {
  // 给索引树上锁
  std::scoped_lock lock{root_latch_};
  // 查找key值应该插入到哪个叶子节点
  auto leaf_pair = find_leaf_page(key, Operation::INSERT, transaction);
  auto leaf_node = leaf_pair.first;
  if (!leaf_node) {
    throw IndexEntryNotFoundError();
  }
  // 检查是否唯一
  if (leaf_node->exists_key(key)) {
    throw InternalError("Non-unique index");
  }

  // 在该叶子节点中插入键值对
  int key_count_before = leaf_node->get_size();
  int key_count_after = leaf_node->insert(key, value);

  // 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
  if (key_count_before == key_count_after) {
    // 重复key，属于异常情况
    release_node(leaf_node, false);
    std::cout << "[Internal] Insert Duplicate Key!!" << std::endl;
    return -1;
  } else if (leaf_node->get_size() >= leaf_node->get_max_size()) {
    auto *new_node = split(leaf_node);
    if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
      file_hdr_->last_leaf_ = new_node->get_page_no();
    }
    insert_into_parent(leaf_node, new_node->get_key(0), new_node, transaction);
    release_node(new_node, true);
  }

  release_node(leaf_node, true);
  return 1; // unused
}
auto IxIndexHandle::delete_entry(const char *key, Transaction *transaction) -> bool {
  std::scoped_lock lock{root_latch_};
  // 获取该键值对所在的叶子结点
  auto leaf_pair = find_leaf_page(key, Operation::DELETE, transaction);
  auto leaf_node = leaf_pair.first;
  bool root_is_latched = leaf_pair.second;
  // 在该叶子结点中删除键值对
  if (!leaf_node) {
    return false;
  }

  int key_count_before = leaf_node->get_size();
  int key_count_after = leaf_node->remove(key);

  if (key_count_before == key_count_after) {
    // 未发生删除
    release_node(leaf_node, false);
    return false;
  } else {
    // 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    coalesce_or_redistribute(leaf_node, transaction, &root_is_latched);
    release_node(leaf_node, true);
    return true;
  }
  // 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
}
auto IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction,
                                             bool *root_is_latched) -> bool {
  if (node->is_root_page()) {
    // 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    return adjust_root(node);
  } else if (node->get_size() >= node->get_min_size()) {
    // 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false
    maintain_parent(node);
    return false;
  }

  // 不满足以上条件则说明：当前节点需要从兄弟节点中借用数据或者与兄弟节点合并以满足B+树平衡条件
  // 获取node结点的父亲结点
  auto parent = fetch_node(node->get_parent_page_no());
  // 寻找node结点的兄弟结点（优先选取前驱结点）
  int index = parent->find_child(node);
  auto neighbor_node = fetch_node((index == 0) ? parent->value_at(1) : parent->value_at(index - 1));

  if (node->get_size() + neighbor_node->get_size() >= node->get_min_size() * 2) {
    redistribute(neighbor_node, node, parent, index);
    release_node(parent, true);
    release_node(neighbor_node, true);
    return false;
  }
  // 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
  else {
    coalesce(&neighbor_node, &node, &parent, index, transaction, root_is_latched);
    release_node(parent, true);
    release_node(neighbor_node, true);
    return true;
  }
}
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
  // 通过index判断neighbor_node是否为node的前驱结点
  // 从neighbor_node中移动一个键值对到node结点中
  // 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
  // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
  if (index == 0) {
    node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
    neighbor_node->erase_pair(0);
    // 修改parent中nerghbor_node的key
    // parent->set_key(1, neighbor_node->get_key(0));
    maintain_parent(neighbor_node);
    maintain_child(node, node->get_size() - 1);
  } else {
    node->insert_pair(0, neighbor_node->get_key(neighbor_node->get_size() - 1),
                      *neighbor_node->get_rid(neighbor_node->get_size() - 1));
    neighbor_node->erase_pair(neighbor_node->get_size() - 1);

    // parent->set_key(index, node->get_key(0));
    maintain_child(node, node->get_size() - 1);
    maintain_parent(node);
  }
}

auto IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) -> bool {
  // 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
  if (index == 0) {
    IxNodeHandle **t = node;
    node = neighbor_node;
    neighbor_node = t;
    index++;
  }

  // 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
  int before_size = (*neighbor_node)->get_size();
  (*neighbor_node)->insert_pairs(before_size, (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());
  int after_size = (*neighbor_node)->get_size();
  for (int i = before_size; i < after_size; ++i) {
    maintain_child(*neighbor_node, i);
  }

  // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
  if ((*node)->get_page_no() == file_hdr_->last_leaf_) {
    file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
  }

  // 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
  if ((*node)->is_leaf_page()) {
    erase_leaf(*node);
  } else {
    buffer_pool_manager_->delete_page({this->get_fd(), (*node)->get_page_no()});
  }
  (*parent)->erase_pair(index);

  return coalesce_or_redistribute(*parent, transaction, root_is_latched);
}

auto IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) -> bool {
  // 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
  if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
    auto new_root_node = fetch_node(old_root_node->value_at(0));
    new_root_node->set_parent_page_no(INVALID_PAGE_ID);
    file_hdr_->root_page_ = new_root_node->get_page_no();
    buffer_pool_manager_->unpin_page(new_root_node->get_page_id(), true);
    release_node_handle(*old_root_node);
    delete new_root_node;
    return true;
  }
  // 如果old_root_node是叶结点，且大小为0，则直接更新root page
  else if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
    release_node_handle(*old_root_node);
    file_hdr_->root_page_ = INVALID_PAGE_ID;
    return true;
  }
  // 除了上述两种情况，不需要进行操作
  return false;
}

auto IxIndexHandle::get_rid(const Iid &iid) const -> Rid {
  IxNodeHandle *node = fetch_node(iid.page_no);
  if (iid.slot_no >= node->get_size()) {
    throw IndexEntryNotFoundError();
  }
  buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
  auto rid = *(node->get_rid(iid.slot_no));
  delete node;
  return rid;
}

auto IxIndexHandle::lower_bound(const char *key) -> Iid {
  std::scoped_lock latch{root_latch_};

  auto node_pair = find_leaf_page(key, Operation::FIND, nullptr);
  if (!node_pair.first) {
    return Iid{-1, -1};
  } else {
    auto *node = node_pair.first;
    int key_idx = node->lower_bound(key);
    Iid iid = {.page_no = node->get_page_no(), .slot_no = key_idx};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    delete node;
    return iid;
  }
}

auto IxIndexHandle::upper_bound(const char *key) -> Iid {
  std::scoped_lock latch{root_latch_};

  auto node_pair = find_leaf_page(key, Operation::FIND, nullptr);
  if (!node_pair.first) {
    return Iid{-1, -1};
  } else {
    auto *node = node_pair.first;
    int key_idx = node->upper_bound(key);
    Iid iid;
    if (key_idx == node->get_size()) {
      iid = leaf_end();
    } else {
      iid = {.page_no = node->get_page_no(), .slot_no = key_idx};
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    delete node;
    return iid;
  }
}

auto IxIndexHandle::leaf_end() -> Iid {
  IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
  Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
  buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
  delete node;
  return iid;
}

auto IxIndexHandle::leaf_begin() -> Iid {
  Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
  return iid;
}

auto IxIndexHandle::fetch_node(int page_no) const -> IxNodeHandle * {
  Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
  assert(page != nullptr);
  return new IxNodeHandle(file_hdr_, page);
}

auto IxIndexHandle::create_node() -> IxNodeHandle * {
  PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
  Page *page = buffer_pool_manager_->new_page(&new_page_id);
  assert(page != nullptr);
  file_hdr_->num_pages_++;
  return new IxNodeHandle(file_hdr_, page);
}

void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
  IxNodeHandle *curr = node;
  while (curr->get_parent_page_no() != IX_NO_PAGE) {
    IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
    int rank = parent->find_child(curr);
    char *parent_key = parent->get_key(rank);
    char *child_first_key = curr->get_key(0);
    if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
      assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
      break;
    }
    memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_); // 修改了parent node
    if (curr != node) {
      delete curr;
    }
    curr = parent;
    assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
  }

  if (curr != node) {
    delete curr;
  }
}

void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
  assert(leaf->is_leaf_page());

  IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
  prev->set_next_leaf(leaf->get_next_leaf());
  buffer_pool_manager_->unpin_page(prev->get_page_id(), true);
  delete prev;

  IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
  next->set_prev_leaf(leaf->get_prev_leaf()); // 注意此处是SetPrevLeaf()
  buffer_pool_manager_->unpin_page(next->get_page_id(), true);
  delete next;
}

void IxIndexHandle::release_node_handle(IxNodeHandle &node) { file_hdr_->num_pages_--; }

void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
  if (!node->is_leaf_page()) {
    //  Current node is inner node, load its child and set its parent to current node
    int child_page_no = node->value_at(child_idx);
    IxNodeHandle *child = fetch_node(child_page_no);
    child->set_parent_page_no(node->get_page_no());
    buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    delete child;
  }
}

auto IxIndexHandle::release_node(IxNodeHandle *node_handle, bool dirty) -> void {
  buffer_pool_manager_->unpin_page(node_handle->get_page_id(), dirty);
  delete node_handle;
}

auto IxIndexHandle::rebuild_index_from_load(char *key, Rid value) -> void {
  auto cur_node = fetch_node(file_hdr_->last_leaf_);
  cur_node->insert(key, value);
  if (cur_node->get_size() >= cur_node->get_max_size()) {
    auto *new_node = split(cur_node);
    file_hdr_->last_leaf_ = new_node->get_page_no();
    insert_into_parent(cur_node, new_node->get_key(0), new_node, nullptr);
    release_node(new_node, true);
  }
  release_node(cur_node, true);
}
