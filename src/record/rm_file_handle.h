#pragma once
#include "storage/buffer_pool_manager.h"

#include <cassert>

#include <memory>
#include <utility>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"

class RmManager;

/* 对表数据文件中的页面进行封装 */
struct RmPageHandle {
  const RmFileHdr *file_hdr; // 当前页面所在文件的文件头指针
  Page *page;                // 页面的实际数据，包括页面存储的数据、元信息等
  RmPageHdr *page_hdr; // page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
  char *bitmap; // page->data的第二部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
  char *slots; // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size

  RmPageHandle(const RmFileHdr *fhdr_, Page *page_)
      : file_hdr(fhdr_), page(page_), page_hdr(reinterpret_cast<RmPageHdr *>(page->get_data())),
        bitmap(page->get_data() + sizeof(RmPageHdr)), slots(bitmap + file_hdr->bitmap_size) {}

  // 返回指定slot_no的slot存储收地址
  char *get_slot(int slot_no) const {
    return slots + slot_no * file_hdr->record_size; // slots的首地址 + slot个数 * 每个slot的大小(每个record的大小)
  }
};

/* 每个RmFileHandle对应一个表的数据文件，里面有多个page，每个page的数据封装在RmPageHandle中 */
class RmFileHandle {
  friend class RmScan;
  friend class RmManager;

private:
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;
  int fd_;               // 打开文件后产生的文件句柄
  RmFileHdr file_hdr_{}; // 文件头，维护当前表文件的元数据

public:
  RmFileHandle(std::shared_ptr<DiskManager> disk_manager, std::shared_ptr<BufferPoolManager> buffer_pool_manager,
               int fd)
      : disk_manager_(std::move(disk_manager)), buffer_pool_manager_(std::move(buffer_pool_manager)), fd_(fd) {
    disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, reinterpret_cast<char *>(&file_hdr_), sizeof(file_hdr_));
    disk_manager_->set_fd_2_page_no(fd, file_hdr_.num_pages);
  }

  RmFileHdr get_file_hdr() { return file_hdr_; }

  RmFileHdr *get_file_hdr_ptr() { return &file_hdr_; }

  int GetFd() { return fd_; }

  /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
  bool is_record(const Rid &rid) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    return Bitmap::is_set(page_handle.bitmap, rid.slot_no);
  }

  std::unique_ptr<RmRecord> get_record(const Rid &rid, Context *context) const;

  Rid insert_record(char *buf, Context *context);

  void delete_record(const Rid &rid, Context *context);

  void update_record(const Rid &rid, char *buf, Context *context);

  void load_batch_record(char *buf, int record_count, int tab_fd, Context *context);

  RmPageHandle create_new_page_handle();

  RmPageHandle fetch_page_handle(int page_no) const;

private:
  RmPageHandle create_page_handle();
};
