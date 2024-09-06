#include "executor_insert.h"

InsertExecutor::InsertExecutor(SmManager *sm_manager,
                               const std::string &tab_name,
                               std::vector<Value> values, Context *context) {
  sm_manager_ = sm_manager;
  tab_ = sm_manager_->db_.get_table(tab_name);
  values_ = values;
  tab_name_ = tab_name;
  if (values.size() != tab_.cols.size()) {
    throw InvalidValueCountError();
  }
  fh_ = sm_manager_->fhs_.at(tab_name).get();
  context_ = context;
}

auto InsertExecutor::getType() -> std::string { return "InsertExecutor"; }

auto InsertExecutor::Next() -> std::unique_ptr<RmRecord> {
  RmRecord rec(fh_->get_file_hdr().record_size);
  for (size_t i = 0; i < values_.size(); i++) {
    auto &col = tab_.cols[i];
    auto &val = values_[i];
    std::unique_ptr<Value> new_val(insert_compatible(col.type, val));
    if (new_val == nullptr) {
      throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
    }
    new_val->init_raw(col.len);
    memcpy(rec.data + col.offset, new_val->raw->data, col.len);
  }
  // Insert into record file
  rid_ = fh_->insert_record(rec.data, context_);
  context_->txn_->append_table_write_record(new TableWriteRecord(WType::INSERT_TUPLE, tab_name_, rid_));

  // 没有事务恢复部分，我们先用异常处理record和索引的一致性问题
  try {
    // Insert into index
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
      auto &index = tab_.indexes[i];
      auto ih = sm_manager_->ihs_
                    .at(sm_manager_->get_ix_manager()->get_index_name(
                        tab_name_, index.cols))
                    .get();
      char *key = new char [index.col_tot_len];
      ix_make_key(key, rec.data, index);
      ih->insert_entry(key, rid_, context_->txn_.get());
      IndexWriteRecord *index_rcd = new IndexWriteRecord(WType::INSERT_TUPLE, tab_name_, rid_, key, index.col_tot_len);
      context_->txn_->append_index_write_record(index_rcd);
      delete[] key;
    }
  } catch (InternalError &error) {
    fh_->delete_record(rid_, context_);
    throw InternalError("Non-unique index!");
  }

  return nullptr;
}

auto InsertExecutor::rid() -> Rid & { return rid_; }
