/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "exexution_conddep.h"

class UpdateExecutor : public AbstractExecutor, public ConditionDependedExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    // std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    // SmManager *sm_manager_;

public:
    UpdateExecutor(SmManager* sm_manager,
                   const std::string& tab_name,
                   std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds,
                   std::vector<Rid> rids,
                   Context* context);

    void beginTuple() override {
        if (context_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
        }
    };

    auto getType() -> std::string override;

    auto cols() const -> const std::vector<ColMeta>& override;

    /**
     * 重写Next方法，用于迭代并更新记录。
     * @return 返回一个unique_ptr指向更新后的记录。
     */
    auto Next() -> std::unique_ptr<RmRecord> override;

    auto rid() -> Rid& override;
};
