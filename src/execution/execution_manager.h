/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/context.h"
#include "execution_defs.h"
#include "executor_abstract.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"
#include "transaction/transaction_manager.h"

class Planner;

class QlManager {
private:
    SmManager* sm_manager_;
    TransactionManager* txn_mgr_;

public:
    QlManager(SmManager* sm_manager, TransactionManager* txn_mgr)
        : sm_manager_(sm_manager)
        , txn_mgr_(txn_mgr) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context* context);
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t* txn_id, Context* context);
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                     std::vector<TabCol> sel_cols,
                     Context* context);

    void run_dml(std::unique_ptr<AbstractExecutor> exec);

    void aggregate_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                               std::vector<TabCol> sel_cols,
                               Context* context,
                               std::vector<Condition> having_clauses,
                               std::vector<TabCol> group_by_cols);
    void
    aggregate_select_from_with_index(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                                     std::vector<TabCol> sel_cols,
                                     Context* context);
    void fast_count_aggre(std::vector<TabCol> sel_cols, Context* context);
    void fast_aggre_with_index(std::vector<TabCol> sel_cols,
                               IxIndexHandle* ix_index_handle,
                               IndexMeta index_meta,
                               std::vector<Condition> conds,
                               RmFileHandle* rm_file_handle,
                               Context* context);
};
