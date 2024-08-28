#pragma once
#include "common/config.h"
#include "parser/ast.h"
#include "system/sm_manager.h"
#include <memory>
class Query {
public:
  std::shared_ptr<ast::TreeNode> parse;
  // where条件
  std::vector<Condition> conds;
  // 投影列
  std::vector<TabCol> cols;
  // 表名
  std::vector<std::string> tables;
  // update 的set 值
  std::vector<SetClause> set_clauses;
  // insert 的values值
  std::vector<Value> values;
  // Having子句
  std::vector<Condition> having_clauses;
  // Group by cols
  std::vector<TabCol> group_by_cols;

  Query() = default;
};

class Analyze {
public:
  Analyze(SmManager *sm_manager, DiskManager *disk_manager) : sm_manager_(sm_manager), disk_manager_(disk_manager) {}
  ~Analyze() = default;

  auto do_analyze(std::shared_ptr<ast::TreeNode> root) -> std::shared_ptr<Query>;

private:
  SmManager *sm_manager_;
  DiskManager *disk_manager_;
  TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);
  TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target, std::vector<TabCol> group_by_cols);
  void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);
  void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
  void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);
  Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
  CompOp convert_sv_comp_op(ast::SvCompOp op);
  bool comparable(ColType type1, ColType type2);
  AggreOp convert_sv_aggre_op(ast::SvAggreType type);
};
