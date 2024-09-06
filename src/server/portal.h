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

#include "common/config.h"
#include "execution/execution_load.h"
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_blocknestedloop_join.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "optimizer/plan.h"
#include <cerrno>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY,
    PORTAL_AGG_SELECT,
    PORTAL_AGG_SELECT_WITH_INDEX,
    PORTAL_FAST_AGG,
    PORTAL_FAST_AGG_WITH_INDEX
} portalTag;

struct PortalStmt {
    portalTag tag;

    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;
    std::vector<Condition> having_clauses;
    std::vector<TabCol> group_by_cols;
    IxIndexHandle* ix_index_handle = nullptr;
    RmFileHandle* rm_file_handle = nullptr;
    IndexMeta index_meta;
    std::vector<Condition> conds;

    PortalStmt(portalTag tag_,
               std::vector<TabCol> sel_cols_,
               std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_)
        : tag(tag_)
        , sel_cols(std::move(sel_cols_))
        , root(std::move(root_))
        , plan(std::move(plan_)) {}
    PortalStmt(portalTag tag_,
               std::vector<TabCol> sel_cols_,
               std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_,
               std::vector<Condition> having_clauses_)
        : tag(tag_)
        , sel_cols(std::move(sel_cols_))
        , root(std::move(root_))
        , plan(std::move(plan_))
        , having_clauses(std::move(having_clauses_)) {}
    PortalStmt(portalTag tag_,
               std::vector<TabCol> sel_cols_,
               std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_,
               std::vector<Condition> having_clauses_,
               std::vector<TabCol> group_by_cols_)
        : tag(tag_)
        , sel_cols(std::move(sel_cols_))
        , root(std::move(root_))
        , plan(std::move(plan_))
        , having_clauses(std::move(having_clauses_))
        , group_by_cols(std::move(group_by_cols_)) {}
    PortalStmt(portalTag tag_,
               std::vector<TabCol> sel_cols_,
               std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_,
               IxIndexHandle* ix_index_handle_,
               IndexMeta index_meta_,
               std::vector<Condition> conds,
               RmFileHandle* rm_file_handle_)
        : tag(tag_)
        , sel_cols(std::move(sel_cols_))
        , root(std::move(root_))
        , plan(std::move(plan_))
        , ix_index_handle(ix_index_handle_)
        , index_meta(index_meta_)
        , conds(conds)
        , rm_file_handle(rm_file_handle_) {}
};

class Portal {
private:
    SmManager* sm_manager_;

public:
    Portal(SmManager* sm_manager)
        : sm_manager_(sm_manager) {}
    ~Portal() {}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context* context) {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY,
                                                std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(),
                                                plan);
        } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY,
                                                std::vector<TabCol>(),
                                                std::unique_ptr<AbstractExecutor>(),
                                                plan);
        } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch (x->tag) {
            case T_select: {
                std::shared_ptr<ProjectionPlan> p =
                    std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                std::unique_ptr<AbstractExecutor> root =
                    convert_plan_executor(p, context);
                std::shared_ptr<ScanPlan> scan =
                    std::dynamic_pointer_cast<ScanPlan>(p->subplan_);

                if (scan != nullptr) {
                    if (scan->conds_.empty() && p->sel_cols_.size() == 1
                        && p->sel_cols_[0].ag_type == ast::SV_AGGRE_COUNT
                        && x->group_by_cols_.empty()) {
                        // 短路结构
                        return std::make_shared<PortalStmt>(PORTAL_FAST_AGG,
                                                            std::move(p->sel_cols_),
                                                            std::move(root),
                                                            plan);
                    }
                }

                if (x->having_clauses_.empty() && x->group_by_cols_.empty()
                    && scan != nullptr) {
                    if (scan->is_index_exist() && p->sel_cols_.size() == 1
                        && p->sel_cols_[0].ag_type != ast::SV_AGGRE_NONE
                        && p->sel_cols_[0].ag_type != ast::SV_AGGRE_SUM) {
                        bool agg_col_in_index = false;
                        bool where_col_not_in_index = false;
                        for (auto& col: scan->index_meta_.cols) {
                            if (col.name == p->sel_cols_[0].col_name) {
                                agg_col_in_index = true;
                            }
                        }
                        for (auto& cond: scan->conds_) {
                            where_col_not_in_index = true;
                            for (auto col: scan->index_meta_.cols) {
                                if (cond.lhs_col.col_name == col.name) {
                                    where_col_not_in_index = false;
                                }
                            }
                        }
                        if (agg_col_in_index && !where_col_not_in_index) {
                            return std::make_shared<PortalStmt>(
                                PORTAL_FAST_AGG_WITH_INDEX,
                                std::move(p->sel_cols_),
                                std::move(root),
                                plan,
                                sm_manager_->ihs_
                                    .at(sm_manager_->get_ix_manager()->get_index_name(
                                        scan->tab_name_, scan->index_meta_.cols))
                                    .get(),
                                scan->index_meta_,
                                scan->conds_,
                                sm_manager_->fhs_.at(scan->tab_name_).get());
                        }
                    }
                }

                if (!x->having_clauses_.empty() || !x->group_by_cols_.empty()) {
                    if (scan->is_index_exist()) {
                        // 如果索引存在
                        return std::make_shared<PortalStmt>(PORTAL_AGG_SELECT_WITH_INDEX,
                                                            std::move(p->sel_cols_),
                                                            std::move(root),
                                                            plan);
                    }
                    std::vector<Condition> having_clauses = x->having_clauses_;
                    std::vector<TabCol> group_by_cols = x->group_by_cols_;
                    return std::make_shared<PortalStmt>(PORTAL_AGG_SELECT,
                                                        std::move(p->sel_cols_),
                                                        std::move(root),
                                                        plan,
                                                        having_clauses,
                                                        group_by_cols);
                }

                for (const auto& sel_col: p->sel_cols_) {
                    // 遍历所有select的selector
                    // 如果有ag_type不为NONE，即拥有聚合类型的
                    // 就返回聚合类型的select
                    if (sel_col.ag_type != ast::SV_AGGRE_NONE) {
                        if (scan->is_index_exist()) {
                            // 如果索引存在
                            return std::make_shared<PortalStmt>(
                                PORTAL_AGG_SELECT_WITH_INDEX,
                                std::move(p->sel_cols_),
                                std::move(root),
                                plan);
                        }
                        std::vector<Condition> having_clauses = x->having_clauses_;
                        std::vector<TabCol> group_by_cols = x->group_by_cols_;
                        return std::make_shared<PortalStmt>(PORTAL_AGG_SELECT,
                                                            std::move(p->sel_cols_),
                                                            std::move(root),
                                                            plan,
                                                            having_clauses,
                                                            group_by_cols);
                    }
                }
                // 全部遍历完发现没有聚合类型的 则说明是普通的select
                // todo: 优化 除去依次遍历 有没有更好的方式？
                return std::make_shared<PortalStmt>(
                    PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
            }

            case T_Update: {
                std::unique_ptr<AbstractExecutor> scan =
                    convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT,
                                                    std::vector<TabCol>(),
                                                    std::move(root),
                                                    plan);
            }
            case T_Delete: {
                std::unique_ptr<AbstractExecutor> scan =
                    convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }

                std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(
                    sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT,
                                                    std::vector<TabCol>(),
                                                    std::move(root),
                                                    plan);
            }

            case T_Insert: {
                std::unique_ptr<AbstractExecutor> root = std::make_unique<InsertExecutor>(
                    sm_manager_, x->tab_name_, x->values_, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT,
                                                    std::vector<TabCol>(),
                                                    std::move(root),
                                                    plan);
            }

            case T_Load: {
                std::unique_ptr<AbstractExecutor> root = std::make_unique<LoadExecutor>(
                    sm_manager_, x->tab_name_, x->values_, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT,
                                                    std::vector<TabCol>(),
                                                    std::move(root),
                                                    plan);
            }

            default:
                throw InternalError("Unexpected field type");
                break;
            }
        } else {
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal,
             QlManager* ql,
             txn_id_t* txn_id,
             Context* context) {
        switch (portal->tag) {
        case PORTAL_ONE_SELECT: {
            ql->select_from(
                std::move(portal->root), std::move(portal->sel_cols), context);
            break;
        }
        case PORTAL_DML_WITHOUT_SELECT: {
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY: {
            ql->run_mutli_query(portal->plan, context);
            break;
        }
        case PORTAL_CMD_UTILITY: {
            ql->run_cmd_utility(portal->plan, txn_id, context);
            break;
        }
        case PORTAL_AGG_SELECT: {
            std::vector<Condition> having_clauses = portal->having_clauses;
            std::vector<TabCol> group_by_cols = portal->group_by_cols;
            ql->aggregate_select_from(std::move(portal->root),
                                      std::move(portal->sel_cols),
                                      context,
                                      having_clauses,
                                      group_by_cols);
            break;
        }
        case PORTAL_AGG_SELECT_WITH_INDEX: {
            ql->aggregate_select_from_with_index(
                std::move(portal->root), std::move(portal->sel_cols), context);
            break;
        }
        case PORTAL_FAST_AGG: {
            ql->fast_count_aggre(std::move(portal->sel_cols), context);
            break;
        }
        case PORTAL_FAST_AGG_WITH_INDEX: {
            ql->fast_aggre_with_index(std::move(portal->sel_cols),
                                      portal->ix_index_handle,
                                      portal->index_meta,
                                      portal->conds,
                                      portal->rm_file_handle,
                                      context);
            break;
        }
        default: {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan,
                                                            Context* context) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return std::make_unique<ProjectionExecutor>(
                convert_plan_executor(x->subplan_, context), x->sel_cols_);
        } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if (x->tag == T_SeqScan) {

                auto* scan = new SeqScanExecutor(
                    sm_manager_, x->tab_name_, x->conds_, context, x->having_clauses_);

                std::unique_ptr<AbstractExecutor> up(scan);
                return up;
            } else {
                return std::make_unique<IndexScanExecutor>(sm_manager_,
                                                           x->tab_name_,
                                                           x->conds_,
                                                           x->index_col_names_,
                                                           x->index_meta_,
                                                           context);
            }
        } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::unique_ptr<AbstractExecutor> left =
                convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right =
                convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join =
                std::make_unique<BlockNestedLoopJoinExecutor>(
                    std::move(left), std::move(right), std::move(x->conds_));
            return join;
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_unique<SortExecutor>(
                convert_plan_executor(x->subplan_, context), x->order_cols_, x->limit_);
        }
        return nullptr;
    }
};
