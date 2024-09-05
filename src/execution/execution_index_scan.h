#pragma once

#include "common/config.h"
#include "execution_abstract.h"
#include "execution_conddep.h"
#include "execution_load.h"
#include "execution_manager.h"
#include "index/ix_defs.h"
#include "system/sm_meta.h"
#include <index/ix_scan.h>

class IndexScanExecutor : public AbstractExecutor, public ConditionDependedExecutor {
private:
    // std::string tab_name_;                      // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据
    IxIndexHandle* ixh_;                       // 索引句柄
    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    // SmManager *sm_manager_;

public:
    IndexScanExecutor(std::shared_ptr<SmManager> sm_manager,
                      std::string tab_name,
                      std::vector<Condition> conds,
                      std::vector<std::string> index_col_names,
                      IndexMeta index_meta,
                      std::shared_ptr<Context> context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = index_col_names;
        index_meta_ = index_meta;
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        ixh_ = sm_manager_->ihs_
                   .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,
                                                                     index_meta_.cols))
                   .get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ},
            {OP_NE, OP_NE},
            {OP_LT, OP_GT},
            {OP_GT, OP_LT},
            {OP_LE, OP_GE},
            {OP_GE, OP_LE},
        };

        for (auto& cond: conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;

        if (context_ != nullptr) {
            context_->lock_mgr_->lock_shared_on_table(context_->txn_.get(), fh_->GetFd());
        }
    }

    std::string getType() override { return "IndexScanExecutor"; }

    const std::vector<ColMeta>& cols() const override { return cols_; }

    size_t tupleLen() const override { return len_; }

    void beginTuple() override {
        while (true) {
            pthread_mutex_lock(LoadExecutor::load_pool_access_lock);
            if (LoadExecutor::load_pool.count(tab_name_) == 0) {
                pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
                break;
            }
            pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
        }
        // 确定上下界
        char* lower_key = new char[index_meta_.col_tot_len];
        char* upper_key = new char[index_meta_.col_tot_len];
        size_t off = 0;
        for (auto& col: index_meta_.cols) {
            Value upper_value, lower_value;
            int col_len = 0;
            // 初始化上下界
            switch (col.type) {
            case TYPE_INT: {
                upper_value.set_int(INT32_MAX);
                lower_value.set_int(INT32_MIN);
                break;
            }
            case TYPE_FLOAT: {
                upper_value.set_float(__FLT_MAX__);
                lower_value.set_float(-__FLT_MAX__);
                break;
            }
            case TYPE_STRING: {
                upper_value.set_string(std::string(col.len, 255));
                lower_value.set_string(std::string(col.len, 0));
                break;
            }
            default: {
                throw InvalidTypeError();
            }
            }

            // 根据条件调整上下界
            for (auto& cond: conds_) {
                if (cond.lhs_col.col_name == col.name && cond.is_rhs_val) {
                    switch (cond.op) {
                    case OP_EQ: {
                        if (check_cond(cond.rhs_val, lower_value, OP_GT)) {
                            lower_value = cond.rhs_val;
                        }
                        if (check_cond(cond.rhs_val, upper_value, OP_LT)) {
                            upper_value = cond.rhs_val;
                        }
                        break;
                    }
                    case OP_GT: {
                    }
                    case OP_GE: {
                        if (check_cond(cond.rhs_val, lower_value, OP_GT)) {
                            lower_value = cond.rhs_val;
                        }
                        break;
                    }
                    case OP_LT: {
                    }
                    case OP_LE: {
                        if (check_cond(cond.rhs_val, upper_value, OP_LT)) {
                            upper_value = cond.rhs_val;
                        }
                        break;
                    }
                    case OP_NE: {
                        break;
                    }
                    default: {
                        throw InternalError("Unexpected CompOp field type");
                    }
                    }
                    break;
                }
            }

            // 生成原始值
            upper_value.init_raw(col.len);
            lower_value.init_raw(col.len);

            memcpy(upper_key + off, upper_value.raw->data, col.len);
            memcpy(lower_key + off, lower_value.raw->data, col.len);
            off += col.len;
        }

        auto lower_id = ixh_->lower_bound(lower_key);
        auto upper_id = ixh_->upper_bound(upper_key);
        APPEND_IID_TO_FILE(lower_id.page_no, lower_id.slot_no, "LOW");
        APPEND_IID_TO_FILE(upper_id.page_no, upper_id.slot_no, "UP");

        scan_ = std::make_unique<IxScan>(
            ixh_, lower_id, upper_id, sm_manager_->get_bpm().get());

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid_, context_.get());
            bool flag = true;

            for (auto cond: conds_) {
                auto col = *get_col(cols_, cond.lhs_col);
                auto val = get_record_value(rec, col);

                if (cond.is_rhs_val && !check_cond(val, cond.rhs_val, cond.op)) {
                    flag = false;
                    break;
                }
            }

            if (flag) {
                break;
            } else {
                scan_->next();
            }
        }
    }

    void nextTuple() override {
        assert(!is_end());
        scan_->next();

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid_, context_.get());
            bool flag = true;

            for (auto cond: conds_) {
                auto col = *get_col(cols_, cond.lhs_col);
                auto val = get_record_value(rec, col);

                if (cond.is_rhs_val && !check_cond(val, cond.rhs_val, cond.op)) {
                    flag = false;
                    break;
                }
            }

            if (flag) {
                break;
            } else {
                scan_->next();
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return fh_->get_record(rid_, context_.get());
    }

    bool is_end() const override { return scan_->is_end(); }

    Rid& rid() override { return rid_; }
};
