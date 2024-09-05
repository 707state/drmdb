#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
#include <execution/execution_load.h>
class SeqScanExecutor : public AbstractExecutor, public ConditionDependedExecutor {
private:
    // std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

    std::vector<Condition> having_clauses_; // 用于having判断

    Rid rid_{};
    std::unique_ptr<RecScan> scan_; // table_iterator

    bool isend;

public:
    SeqScanExecutor(std::shared_ptr<SmManager> sm_manager,
                    std::string tab_name,
                    std::vector<Condition> conds,
                    std::shared_ptr<Context> context,
                    std::vector<Condition> having_clauses)
        : conds_(std::move(conds))
        , fh_(sm_manager_->fhs_.at(tab_name_).get())
        , len_(cols_.back().offset + cols_.back().len)
        , fed_conds_(conds_)
        , isend(false) {

        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);

        TabMeta& tab = sm_manager_->db_.get_table(tab_name_);

        cols_ = tab.cols;

        context_ = context;

        auto* scan = new RmScan(fh_);
        scan_ = std::unique_ptr<RecScan>(scan);

        if (context != nullptr) {
            context->lock_mgr_->lock_shared_on_table(context->txn_.get(), fh_->GetFd());
        }

        having_clauses_ = std::move(having_clauses);
    }

    size_t tupleLen() const override { return len_; }

    std::string getType() override { return "SeqScanExecutor"; }

    const std::vector<ColMeta>& cols() const override { return cols_; }

    void beginTuple() override {
        while (true) {
            pthread_mutex_lock(LoadExecutor::load_pool_access_lock);
            if (LoadExecutor::load_pool.count(tab_name_) == 0) {
                pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
                break;
            }
            pthread_mutex_unlock(LoadExecutor::load_pool_access_lock);
        }
        // 为scan_赋值
        // RmScan *scan = new RmScan(fh_);
        // scan_ = std::unique_ptr<RecScan>(scan);
        scan_->begin();
        auto rec = scan_.get()->rid();
        nextTuple();
        isend = false;
    }

    void nextTuple() override {
        // 先检查此条记录是否满足所有条件
        // 再next下一条
        // 返回的是此条！！！！！！！！！！！！！！！！！
        scan_->next();
        while (!scan_->is_end()) {
            auto rec = scan_.get()->rid();
            auto record = fh_->get_record(rec, context_.get());

            // 检查所有条件
            bool flag = true;

            for (auto& cond: conds_) {
                if (!check_cond(cond, *record)) {
                    flag = false;
                    break;
                }
            }

            if (flag) {
                rid_ = rec;
                // scan_->next();
                return;
            }
            scan_->next();
        }
        isend = true;
    }

    bool is_end() const override { return scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        // nextTuple();
        // if (scan_->is_end()) {
        //     std::cout<<"executor_seq_scan Next is_end"<<std::endl;
        //     return nullptr;
        // }
        //    std::cout<<"getting next from seq scan"<<std::endl;
        auto rec = rid();

        auto record = fh_->get_record(rec, context_.get());

        return record;
    }

    Rid& rid() override { return rid_; }
};
