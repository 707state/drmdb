#pragma once

#include <memory>
#include <string>
#include <utility>

#include "ix_defs.h"
#include "ix_index_handle.h"
#include "system/sm_meta.h"

class IxManager {
private:
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;

public:
  IxManager(std::shared_ptr<DiskManager> disk_manager, std::shared_ptr<BufferPoolManager> buffer_pool_manager)
      : disk_manager_(std::move(disk_manager)), buffer_pool_manager_(std::move(buffer_pool_manager)) {}

  auto get_index_name(const std::string &filename, const std::vector<std::string> &index_cols) -> std::string {
    std::string index_name = filename;
    for (const auto &index_col : index_cols) {
      index_name += "_" + index_col;
    }
    index_name += ".idx";
    return index_name;
  }

  auto get_index_name(const std::string &filename, const std::vector<ColMeta> &index_cols) -> std::string {
    std::string index_name = filename;
    for (const auto &index_col : index_cols) {
      index_name += "_" + index_col.name;
    }
    index_name += ".idx";
    return index_name;
  }

  auto index_exists(const std::string &filename, const std::vector<ColMeta> &index_cols) -> bool {
    auto ix_name = get_index_name(filename, index_cols);
    return disk_manager_->is_file(ix_name);
  }

  auto index_exists(const std::string &filename, const std::vector<std::string> &index_cols) -> bool {
    auto ix_name = get_index_name(filename, index_cols);
    return disk_manager_->is_file(ix_name);
  }

  auto create_index(const std::string &filename, const std::vector<ColMeta> &index_cols) -> void {
    std::string ix_name = get_index_name(filename, index_cols);
    disk_manager_->create_file(ix_name);
    int fd = disk_manager_->open_file(ix_name);

    /**
     * 理论上，|page_hdr| + (|key| + |rid|) * n <= PAGE_SIZE
     * 但是我们保留了一个slot方便插入和删除，所以：
     * |page_hdr| + (|key| + |rid|) * (n + 1) <= PAGE_SIZE
     */
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (const auto &col : index_cols) {
      col_tot_len += col.len;
    }
    if (col_tot_len > IX_MAX_COL_LEN) {
      throw InvalidColLengthError(col_tot_len);
    }
    /**
     * 根据 |page_hdr| + (|key| + |rid|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
     * 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
     */
    int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1);
    assert(btree_order > 2);

    // 创建索引文件头
    auto *p_ix_file_hdr =
        new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE, col_num, col_tot_len, btree_order,
                      (btree_order + 1) * col_tot_len, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
    for (int i = 0; i < col_num; ++i) {
      p_ix_file_hdr->col_types_.emplace_back(index_cols[i].type);
      p_ix_file_hdr->col_lens_.emplace_back(index_cols[i].len);
    }
    p_ix_file_hdr->update_tot_len();

    char *data = new char[PAGE_SIZE];
    // 序列化索引文件头并写入
    p_ix_file_hdr->serialize(data);
    // 索引文件头单占一页
    disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data, PAGE_SIZE);
    delete p_ix_file_hdr;
    delete[] data;

    char page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    // 注意leaf header页号为1，也标记为叶子结点，其前一个/后一个叶子均指向root node
    // Create leaf list header page and write to file
    // 这个leaf list header也单独占一页
    // FIXME 这个leaf header疑似没有任何用处
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto p_page_hdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *p_page_hdr = {
          .parent = IX_NO_PAGE,
          .num_key = 0,
          .is_leaf = true,
          .prev_leaf = IX_INIT_ROOT_PAGE,
          .next_leaf = IX_INIT_ROOT_PAGE,
      };
      disk_manager_->write_page(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
    }
    // 注意root node页号为2，也标记为叶子结点，其前一个/后一个叶子均指向leaf header
    // Create root node and write to file
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto p_page_hdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *p_page_hdr = {
          .parent = IX_NO_PAGE,
          .num_key = 0,
          .is_leaf = true,
          .prev_leaf = IX_LEAF_HEADER_PAGE,
          .next_leaf = IX_LEAF_HEADER_PAGE,
      };
      // Must write PAGE_SIZE here in case of future fetch_node()
      disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
    }

    // Close index file
    disk_manager_->close_file(fd);
  }

  auto destroy_index(const std::string &filename, const std::vector<ColMeta> &index_cols) -> void {
    std::string ix_name = get_index_name(filename, index_cols);
    disk_manager_->destroy_file(ix_name);
  }

  auto destroy_index(const std::string &filename, const std::vector<std::string> &index_cols) -> void {
    std::string ix_name = get_index_name(filename, index_cols);
    disk_manager_->destroy_file(ix_name);
  }

  auto destroy_index(const IxIndexHandle *ih, const std::string &filename,
                     const std::vector<std::string> &index_cols) -> void {
    std::string ix_name = get_index_name(filename, index_cols);
    buffer_pool_manager_->delete_all_pages(ih->fd_);
    disk_manager_->destroy_file(ix_name);
  }

  auto open_index(const std::string &filename,
                  const std::vector<ColMeta> &index_cols) -> std::unique_ptr<IxIndexHandle> {
    std::string ix_name = get_index_name(filename, index_cols);
    int fd = disk_manager_->open_file(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
  }

  auto open_index(const std::string &filename,
                  const std::vector<std::string> &index_cols) -> std::unique_ptr<IxIndexHandle> {
    std::string ix_name = get_index_name(filename, index_cols);
    int fd = disk_manager_->open_file(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
  }

  void close_index(const IxIndexHandle *ih) {
    char *data = new char[ih->file_hdr_->tot_len_];
    ih->file_hdr_->serialize(data);
    disk_manager_->write_page(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->flush_all_pages(ih->fd_);
    disk_manager_->close_file(ih->fd_);
  }
};
