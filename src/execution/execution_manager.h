#pragma once
#include <utility>

#include <utility>

#include "execution/execution_abstract.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"
#include <optimizer/plan.h>
class Planner;

class QlManager {
private:
    std::shared_ptr<SmManager> sm_manager_;
    std::shared_ptr<TransactionManager> txn_mgr_;

public:
    QlManager(std::shared_ptr<SmManager> sm_manager,
              std::shared_ptr<TransactionManager> txn_mgr)
        : sm_manager_(std::move(sm_manager))
        , txn_mgr_(std::move(txn_mgr)) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context* context);
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t* txn_id, Context* context);
    void select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot,
                     std::vector<TabCol> sel_cols,
                     Context* context);

    void run_dml(std::shared_ptr<AbstractExecutor> exec);

    void aggregate_select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot,
                               std::vector<TabCol> sel_cols,
                               Context* context,
                               std::vector<Condition> having_clauses,
                               std::vector<TabCol> group_by_cols);
    void
    aggregate_select_from_with_index(std::shared_ptr<AbstractExecutor> executorTreeRoot,
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
