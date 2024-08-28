#include <cmath>

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置（默认该记录号是合法的）
 * @param {Context*} context 上下文，用于上锁
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const {
  if (context != nullptr) {
    context->lock_mgr_->lock_shared_on_record(context->txn_.get(), rid, fd_);
  }
  // 获取指定记录所在page的handle
  RmPageHandle page_handle = fetch_page_handle(rid.page_no);
  // 初始化一个指向RmRecord的指针（赋值其内部的data和size）
  int size = file_hdr_.record_size;
  auto rm_rcd = std::make_unique<RmRecord>(size);
  rm_rcd->SetData(page_handle.get_slot(rid.slot_no));
  rm_rcd->size = size;
  buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
  return rm_rcd;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char *buf, Context *context) {
  RmPageHandle spare_page_handle = create_page_handle();
  // 在page handle中找到空闲slot位置
  int slot_no = Bitmap::first_bit(false, spare_page_handle.bitmap, file_hdr_.num_records_per_page);
  if (slot_no == file_hdr_.num_records_per_page) {
    throw InvalidSlotNoError(slot_no, file_hdr_.num_records_per_page);
  }
  // 找到合适的位置就上锁
  if (context != nullptr) {
    auto rid = Rid{.page_no = spare_page_handle.page->get_page_id().page_no, .slot_no = slot_no};
    context->lock_mgr_->lock_exclusive_on_record(context->txn_.get(), rid, fd_);
  }

  // 将buf复制到空闲slot位置
  std::memcpy(spare_page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
  Bitmap::set(spare_page_handle.bitmap, slot_no);

  // 更新page_handle.page_hdr中的数据结构
  spare_page_handle.page_hdr->num_records++;
  if (spare_page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    file_hdr_.first_free_page_no = RM_NO_PAGE;
  }

  buffer_pool_manager_->unpin_page(spare_page_handle.page->get_page_id(), true);
  return Rid{spare_page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context) {
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_.get(), rid, fd_);
  }

  RmPageHandle page_handle = fetch_page_handle(rid.page_no);
  if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
    throw RecordNotFoundError(rid.page_no, rid.slot_no);
  }

  Bitmap::reset(page_handle.bitmap, rid.slot_no);
  page_handle.page_hdr->num_records--;

  buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_.get(), rid, fd_);
  }

  RmPageHandle page_handle = fetch_page_handle(rid.page_no);
  if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
    throw RecordNotFoundError(rid.page_no, rid.slot_no);
  }

  std::memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
  buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号（默认该页面号合法）
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
  if (page_no >= RmFileHandle::file_hdr_.num_pages) {
    throw PageNotExistsError("", page_no);
  }
  // 构建Page ID
  PageId pagd_id;
  pagd_id.fd = fd_;
  pagd_id.page_no = page_no;
  // 获取指定的Page，会在buffer pool中pin这个page
  Page *page = buffer_pool_manager_->fetch_page(pagd_id);
  if (page == nullptr) {
    throw PageNotExistsError("", page_no);
  }
  // 返回句柄
  return {&file_hdr_, page};
}

/**
 * @brief 创建一个新的page并返回其handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
  // 使用缓冲池来创建一个新page
  PageId page_id;
  page_id.fd = fd_;
  Page *page = buffer_pool_manager_->new_page(&page_id);
  // 初始化page handle中的相关信息
  RmPageHandle page_handle(&file_hdr_, page);
  Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
  page_handle.page_hdr->num_records = 0;
  // 更新file_hdr_
  file_hdr_.num_pages++;
  file_hdr_.first_free_page_no = page_id.page_no;
  return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
  if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
    // 没有空闲页, 创建一个新page
    return create_new_page_handle();
  } else {
    // 有空闲页：直接获取第一个空闲页
    return fetch_page_handle(file_hdr_.first_free_page_no);
  }
}
