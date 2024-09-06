#include "executor_update.h"
#include "common/config.h"

UpdateExecutor::UpdateExecutor(SmManager *sm_manager,
                               const std::string &tab_name,
                               std::vector<SetClause> set_clauses,
                               std::vector<Condition> conds,
                               std::vector<Rid> rids, Context *context) {
  sm_manager_ = sm_manager;
  tab_name_ = tab_name;
  set_clauses_ = set_clauses;
  tab_ = sm_manager_->db_.get_table(tab_name);
  fh_ = sm_manager_->fhs_.at(tab_name).get();
  conds_ = conds;
  rids_ = rids;
  context_ = context;
}

auto UpdateExecutor::getType() -> std::string { return "UpdateExecutor"; }

auto UpdateExecutor::cols() const -> const std::vector<ColMeta> & {
  return tab_.cols;
}

auto UpdateExecutor::Next() -> std::unique_ptr<RmRecord> {
  bool flag = false; // 标志有更新
  /* 遍历rids_中的每个记录 */
  for (auto &rid : rids_) {
    /* 从文件中获取原始记录 */
    auto record = fh_->get_record(rid, context_);
    /* 创建一个新的RmRecord对象，用于存储更新后的记录 */
    auto new_record = std::make_unique<RmRecord>(*record);

    /* 检查记录是否满足更新条件，如果不满足，则跳过此记录 */
    if (!check_conds(conds_, *record)) {
      continue;
    }
    flag = true;
    /* 遍历表中的每个索引，删除旧的索引条目 */
    /* 删除所有的index */
    try {
      for (auto &index : tab_.indexes) {
        /* 获取索引句柄 */
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        /* 构建索引键 */
        char *key = new char [index.col_tot_len];
        ix_make_key(key, record->data, index);
        /* 从索引中删除条目 */
        ih->delete_entry(key, context_->txn_.get());
        APPEND_TO_FILE(rid.page_no,rid.slot_no,"WRITE");
        /* 创建并添加索引写入记录到事务中 */
        IndexWriteRecord *index_rcd = new IndexWriteRecord(WType::DELETE_TUPLE, tab_name_, rid, key, index.col_tot_len);
        context_->txn_->append_index_write_record(index_rcd);
        delete [] key;
      }

      /* 遍历更新语句中的每个设置子句，更新记录的字段值 */
      /* 设置每个字段 */
      for (auto &set_clause : set_clauses_) {
        auto col = set_clause.lhs;
        auto val = set_clause.rhs;

        if (set_clause.is_plus_value) {
          // 如果满足+的条件
          /* 获取列元数据 */
          auto col_meta = *sm_manager_->db_.get_table(set_clause.lhs.tab_name).get_col(col.col_name);

          /* 更新记录的字段值 */
          int offset = col_meta.offset;
          int len = col_meta.len;
          std::unique_ptr<Value> new_val;

          // 获取原有的值，转换为对应的Value类型
          char* raw_value = new_record->get_column_value(offset, len);

          if (col_meta.type == TYPE_INT) {
            int original_int_value = *reinterpret_cast<int*>(raw_value);
            if (val.type == TYPE_INT) {
              original_int_value += val.int_val;
              new_val = std::make_unique<Value>();
              new_val->set_int(original_int_value);
            } else if (val.type == TYPE_FLOAT) {
              float new_float_value = static_cast<float>(original_int_value) + val.float_val;
              new_val = std::make_unique<Value>();
              new_val->set_float(new_float_value);
            } else {
              throw InternalError("仅支持INT和FLOAT类型执行col=col+value操作");
            }
          } else if (col_meta.type == TYPE_FLOAT) {
            float original_float_value = *reinterpret_cast<float*>(raw_value);
            if (val.type == TYPE_FLOAT) {
              original_float_value += val.float_val;
              new_val = std::make_unique<Value>();
              new_val->set_float(original_float_value);
            } else if (val.type == TYPE_INT) {
              original_float_value += static_cast<float>(val.int_val);
              new_val = std::make_unique<Value>();
              new_val->set_float(original_float_value);
            } else {
              throw InternalError("仅支持INT和FLOAT类型执行col=col+value操作");
            }
          } else {
            throw InternalError("仅支持INT和FLOAT类型执行col=col+value操作");
          }

          if (new_val == nullptr) {
            throw InternalError("需要更新的类型不匹配");
          }

          new_val->init_raw(col_meta.len);
          new_record->set_column_value(offset, len, new_val->raw->data);
        } else {
          // 放在else中而不合并是为了保证原本update的稳定性
          // 新增加的功能不会影响到原本的功能

          /* 获取列元数据 */
          auto col_meta = sm_manager_->db_.get_table(set_clause.lhs.tab_name)
                              .get_col(col.col_name)[0];
          /* 更新记录的字段值 */
          int offset = col_meta.offset;
          int len = col_meta.len;
          std::unique_ptr<Value> new_val(insert_compatible(col_meta.type, val));
          if (new_val == nullptr) {
            throw InternalError("需要更新的类型不匹配");
          }
          new_val->init_raw(col_meta.len);
          new_record->set_column_value(offset, len, new_val->raw->data);
        }
      }

      /* 更新文件中的记录 */
      fh_->update_record(rid, new_record.get()->data, context_);
      APPEND_TO_FILE(rid.page_no, rid.slot_no, "WRITE");

      /* 创建并添加表写入记录到事务中 */
      TableWriteRecord *write_rcd = new TableWriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *record);
      context_->txn_->append_table_write_record(write_rcd);

      /* 尝试为更新后的记录插入新的索引条目 */
      /* 再次遍历表中的每个索引，插入新的索引条目 */
      for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_
                      .at(sm_manager_->get_ix_manager()->get_index_name(
                          tab_name_, index.cols))
                      .get();
        /* 构建新的索引键 */
        char *key = new char [index.col_tot_len];
        ix_make_key(key, new_record->data, index);
        /* 在索引中插入新的条目 */
        ih->insert_entry(key, rid, context_->txn_.get());
        /* 创建并添加索引写入记录到事务中 */
        IndexWriteRecord *index_rcd = new IndexWriteRecord(WType::INSERT_TUPLE, tab_name_, rid, key, index.col_tot_len);
        context_->txn_->append_index_write_record(index_rcd);
        delete[] key;
      }
    } catch (InternalError &error) {
      /* 如果插入索引时发生错误（如索引唯一性冲突），则回滚更新操作 */
      /* 恢复record */
      fh_->update_record(rid, record->data, context_);
      /* 恢复所有的index */
      for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        /* 构建索引键 */
        char *key = new char [index.col_tot_len];
        ix_make_key(key, record->data, index);
        /* 在索引中重新插入条目 */
        ih->insert_entry(key, rid, context_->txn_.get());
        delete[] key;
      }
      /* 重新抛出异常 */
      throw error;
    }
  }

  if (!flag) {
    throw InternalError("更新失败!不符合更新条件!");
  }
  /* 迭代完成后，返回nullptr表示没有更多的记录需要更新 */
  return nullptr;
}

auto UpdateExecutor::rid() -> Rid & { return _abstract_rid; }
