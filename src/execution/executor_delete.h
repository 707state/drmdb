#pragma once

#include "executor_abstract.h"
#include "exexution_conddep.h"

class DeleteExecutor : public AbstractExecutor, public ConditionDependedExecutor {
public:
    DeleteExecutor(SmManager* sm_manager,
                   const std::string& tab_name,
                   std::vector<Condition> conds,
                   std::vector<Rid> rids,
                   Context* context);

    void beginTuple() override {
        if (context_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
        }
    }

    auto getType() -> std::string override;

    auto cols() const -> const std::vector<ColMeta>& override;

    auto Next() -> std::unique_ptr<RmRecord> override;

    auto rid() -> Rid& override;

private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager* sm_manager_;
};
