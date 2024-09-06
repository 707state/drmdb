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

class InsertExecutor : public AbstractExecutor, public ConditionDependedExecutor {
public:
    InsertExecutor(SmManager* sm_manager,
                   const std::string& tab_name,
                   std::vector<Value> values,
                   Context* context);

    void beginTuple() override {
        if (context_ != nullptr) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
        }
    }

    auto getType() -> std::string override;

    auto Next() -> std::unique_ptr<RmRecord> override;

    auto rid() -> Rid& override;

private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle* fh_;          // 表的数据文件句柄
    // std::string tab_name_;          // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
              // SmManager *sm_manager_;
};
