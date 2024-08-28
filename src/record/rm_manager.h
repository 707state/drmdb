#pragma once

#include <cassert>
#include <memory>
#include <utility>

#include "bitmap.h"
#include "record/rm_scan.h"
#include "rm_defs.h"
#include "rm_file_handle.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

/* 记录管理器，用于管理表的数据文件，进行文件的创建、打开、删除、关闭 */
class RmManager {
private:
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;

public:
  RmManager(std::shared_ptr<DiskManager> disk_manager, std::shared_ptr<BufferPoolManager> buffer_pool_manager)
      : disk_manager_(std::move(disk_manager)), buffer_pool_manager_(std::move(buffer_pool_manager)) {}

  /**
   * @description: 创建表的数据文件并初始化相关信息
   * @param {string&} filename 要创建的文件名称
   * @param {int} record_size 表中记录的大小
   */
  void create_file(const std::string &filename, int record_size) {
    if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
      throw InvalidRecordSizeError(record_size);
    }
    disk_manager_->create_file(filename);
    int fd = disk_manager_->open_file(filename);

    // 初始化file header
    RmFileHdr file_hdr{};
    file_hdr.record_size = record_size;
    file_hdr.num_pages = 1;
    file_hdr.first_free_page_no = RM_NO_PAGE;
    // We have: sizeof(hdr) + (n + 7) / 8 (bitmap) + n * record_size <= PAGE_SIZE
    file_hdr.num_records_per_page =
        (BITMAP_WIDTH * (PAGE_SIZE - 1 - static_cast<int>(sizeof(RmFileHdr))) + 1) / (1 + record_size * BITMAP_WIDTH);
    file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

    // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
    disk_manager_->write_page(fd, RM_FILE_HDR_PAGE, reinterpret_cast<char *>(&file_hdr), sizeof(file_hdr));
    disk_manager_->close_file(fd);
  }

  /**
   * @description: 删除表的数据文件
   * @param {string&} filename 要删除的文件名称
   */
  void destroy_file(const std::string &filename) {
    disk_manager_->close_file(disk_manager_->get_file_fd(filename));
    disk_manager_->destroy_file(filename);
  }

  // 注意这里打开文件，创建并返回了record file handle的指针
  /**
   * @description: 打开表的数据文件，并返回文件句柄
   * @param {string&} filename 要打开的文件名称
   * @return {unique_ptr<RmFileHandle>} 文件句柄的指针
   */
  std::unique_ptr<RmFileHandle> open_file(const std::string &filename) {
    int fd = disk_manager_->open_file(filename);
    return std::make_unique<RmFileHandle>(disk_manager_, buffer_pool_manager_, fd);
  }
  /**
   * @description: 关闭表的数据文件
   * @param {RmFileHandle*} file_handle 要关闭文件的句柄
   * WARNING:
   * 这里有一处c风格类型转换，(char))&file_handle->file_hdr_，可能存在问题
   */
  void close_file(const RmFileHandle *file_handle) {
    disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                              sizeof(file_handle->file_hdr_));
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->flush_all_pages(file_handle->fd_);
    disk_manager_->close_file(file_handle->fd_);
  }
};
