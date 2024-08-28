#include "record/rm_defs.h"
#include <memory>
#include <record/rm_manager.h>
#include <record/rm_scan.h>
RmScan::RmScan(RmFileHandle *file_handle) : file_handle_(file_handle) {
  begin();
  next();
}
void RmScan::begin() {
  rid_.page_no = RM_FIRST_RECORD_PAGE;
  rid_.slot_no = -1;
}
/**
 * @brief 找到文件中下一个存放了记录的位置
 */

void RmScan::next() {
  if (is_end()) {
    return;
  }
  for (; rid_.page_no < this->file_handle_->file_hdr_.num_pages; rid_.page_no++) {
  }
}
