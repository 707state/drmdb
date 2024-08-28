#pragma once
#include "system/sm_manager.h"
using PlanTag = enum PlanTag {
  T_Invalid = 1,
  T_Help,
  T_ShowTable,
  T_DescTable,
  T_CreateTable,
  T_DropTable,
  T_CreateIndex,
  T_ShowIndex,
  T_DropIndex,
  T_Insert,
  T_Load,
  T_Update,
  T_Delete,
  T_select,
  T_Transaction_begin,
  T_Transaction_commit,
  T_Transaction_abort,
  T_Transaction_rollback,
  T_SeqScan,
  T_IndexScan,
  T_NestLoop,
  T_Sort,
  T_Projection,
  T_Aggre,            // 增加Aggregation
  T_SetOutputFileOff, // 增加输出语句控制
};

// 查询执行计划
class Plan {
public:
  PlanTag tag;
  virtual ~Plan() = default;
};

class ScanPlan : public Plan {
public:
  ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
           std::vector<std::string> index_col_names)
      : tab_name_(std::move(tab_name)), conds_(std::move(conds)), len_(cols_.back().offset + cols_.back().len),
        fed_conds_(conds_), index_col_names_(index_col_names) {
    Plan::tag = tag;

    TabMeta &tab = sm_manager->db_.get_table(tab_name_);
    cols_ = tab.cols;
  }
  ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
           std::vector<std::string> index_col_names, std::vector<Condition> having_clauses)
      : tab_name_(std::move(tab_name)), conds_(std::move(conds)), len_(cols_.back().offset + cols_.back().len),
        fed_conds_(conds_), index_col_names_(index_col_names), having_clauses_(having_clauses) {
    Plan::tag = tag;

    TabMeta &tab = sm_manager->db_.get_table(tab_name_);
    cols_ = tab.cols;
  }
  ~ScanPlan() {}
  // 以下变量同ScanExecutor中的变量
  std::string tab_name_;
  std::vector<ColMeta> cols_;
  std::vector<Condition> conds_;
  size_t len_;
  std::vector<Condition> fed_conds_;
  std::vector<std::string> index_col_names_;

  // 增加的变量Index_meta
  IndexMeta index_meta_;

  // 增加变量having_clauses_
  std::vector<Condition> having_clauses_;

  bool is_index_exist() {
    // 如果存在返回true
    // 不存在返回false
    return !index_col_names_.empty();
  }
};

class JoinPlan : public Plan {
public:
  JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds)
      : left_(std::move(left)), right_(std::move(right)), conds_(std::move(conds)), type(INNER_JOIN) {
    Plan::tag = tag;
  }
  ~JoinPlan() {}
  // 左节点
  std::shared_ptr<Plan> left_;
  // 右节点
  std::shared_ptr<Plan> right_;
  // 连接条件
  std::vector<Condition> conds_;
  // future TODO: 后续可以支持的连接类型
  JoinType type;
};

class ProjectionPlan : public Plan {
public:
  ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols)
      : subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)) {
    Plan::tag = tag;
  }
  ~ProjectionPlan() {}
  std::shared_ptr<Plan> subplan_;
  std::vector<TabCol> sel_cols_;
};

class SortPlan : public Plan {
public:
  SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<OrderByCol> order_cols, int limit)
      : subplan_(std::move(subplan)), order_cols_(order_cols), limit_(limit) {
    Plan::tag = tag;
  }
  ~SortPlan() {}
  std::shared_ptr<Plan> subplan_;
  std::vector<OrderByCol> order_cols_;
  int limit_;
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan {
public:
  DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values,
          std::vector<Condition> conds, std::vector<SetClause> set_clauses)
      : subplan_(std::move(subplan)), tab_name_(std::move(tab_name)), values_(std::move(values)),
        conds_(std::move(conds)), set_clauses_(std::move(set_clauses)) {
    Plan::tag = tag;
  }
  ~DMLPlan() override = default;
  std::shared_ptr<Plan> subplan_;
  std::string tab_name_;
  std::vector<Value> values_;
  std::vector<Condition> conds_;
  std::vector<SetClause> set_clauses_;
  std::vector<Condition> having_clauses_;
  std::vector<TabCol> group_by_cols_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan {
public:
  DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols)
      : tab_name_(std::move(tab_name)), tab_col_names_(std::move(col_names)), cols_(std::move(cols)) {
    Plan::tag = tag;
  }
  ~DDLPlan() {}
  std::string tab_name_;
  std::vector<std::string> tab_col_names_;
  std::vector<ColDef> cols_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan {
public:
  OtherPlan(PlanTag tag, std::string tab_name) : tab_name_(std::move(tab_name)) { Plan::tag = tag; }
  ~OtherPlan() {}
  std::string tab_name_;
};

class plannerInfo {
public:
  std::shared_ptr<ast::SelectStmt> parse;
  std::vector<Condition> where_conds;
  std::vector<TabCol> sel_cols;
  std::shared_ptr<Plan> plan;
  std::vector<std::shared_ptr<Plan>> table_scan_executors;
  std::vector<SetClause> set_clauses;
  plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : parse(std::move(parse_)) {}
};
