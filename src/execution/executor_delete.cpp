#include "executor_delete.h"

DeleteExecutor::DeleteExecutor(SmManager* sm_manager,
                               const std::string& tab_name,
                               std::vector<Condition> conds,
                               std::vector<Rid> rids,
                               Context* context) {
    sm_manager_ = sm_manager;
    tab_name_ = tab_name;
    tab_ = sm_manager_->db_.get_table(tab_name);
    fh_ = sm_manager_->fhs_.at(tab_name).get();
    conds_ = conds;
    rids_ = rids;
    context_ = context;
}

auto DeleteExecutor::getType() -> std::string { return "DeleteExecutor"; }

auto DeleteExecutor::cols() const -> const std::vector<ColMeta>& { return tab_.cols; }

auto DeleteExecutor::Next() -> std::unique_ptr<RmRecord> {
    for (auto& rid: rids_) {
        auto record = fh_->get_record(rid, context_);
        RmRecord delete_rcd(record->size);
        memcpy(delete_rcd.data, record->data, record->size);
        // 删除index
        // 这里与index插入是相似的，都是对每一个的index，得到对应col的key，调用delete_entry(key,context_->txn_);
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_
                          .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,
                                                                            index.cols))
                          .get();
            char* key = new char[index.col_tot_len];
            ix_make_key(key, record->data, index);
            ih->delete_entry(key, context_->txn_.get());
            IndexWriteRecord* index_rcd = new IndexWriteRecord(
                WType::DELETE_TUPLE, tab_name_, rid, key, index.col_tot_len);
            context_->txn_->append_index_write_record(index_rcd);
            delete[] key;
        }

        // 删除记录
        fh_->delete_record(rid, context_);
        TableWriteRecord* write_record =
            new TableWriteRecord(WType::DELETE_TUPLE, tab_name_, rid, delete_rcd);
        context_->txn_->append_table_write_record(write_record);
    }

    return nullptr;
}

auto DeleteExecutor::rid() -> Rid& { return _abstract_rid; }
