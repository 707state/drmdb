#include "analyze/analyze.h"
#include <memory>
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
  std::shared_ptr<Query> query = std::make_shared<Query>();
  if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
    query->tables = std::move(x->tabs);
    // 检查表名是否存在
    for (auto &table : query->tables) {
      if (!sm_manager_->db_.is_table(table)) {
        throw FileNotFoundError(table);
      }
    }

    // 检查 SELECT 列表中是否出现没有在 GROUP BY 子句中的非聚集列
    if (!x->group_by.empty()) {
      for (const auto &col : x->cols) {
        if (col->ag_type == ast::SV_AGGRE_NONE) {
          bool found = false;
          for (const auto &gb_col : x->group_by) {
            if (col->col_name == gb_col->col_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            throw InternalError("SELECT 列表中不能出现没有在 GROUP BY 子句中的非聚集列");
          }
        }
      }
    }

    // 尝试: 如果没有group by直接having的话抛出错误
    if (x->group_by.empty() && !x->having.empty()) {
      throw InternalError("必须在group by的结果中使用having子句");
    }

    // 检查having子句中是否有不在select list中的聚合结果
    // 处理having子句条件
    if (!x->having.empty()) {
      get_clause(x->having, query->having_clauses);
      for (const auto &having_clause : query->having_clauses) {
        if (having_clause.lhs_col.ag_type != ast::SV_AGGRE_COUNT) {
          // 检查其是否在select list中
          bool found_in_select = false;
          for (const auto &select_col : x->cols) {
            if (having_clause.lhs_col.col_name == select_col->col_name &&
                having_clause.lhs_col.ag_type == select_col->ag_type) {
              found_in_select = true;
              break;
            }
          }
          if (!found_in_select) {
            // 如果列不在SELECT子句中，抛出错误
            throw InternalError("having子句中的聚合结果必须在SELECT LIST中");
          }
        } else if (having_clause.lhs_col.ag_type == ast::SV_AGGRE_COUNT && having_clause.lhs_col.col_name.empty()) {
          // 如果是COUNT(*)聚合函数, 则无需检查
          continue;
        } else {
          // 如果不是COUNT(*)聚合函数, 抛出异常
          throw InternalError(
              "Only COUNT(*) can be used in the HAVING clause without being listed in the SELECT or GROUP BY clauses.");
        }
      }
    }

    // 检查where子句中是否有聚合函数
    for (const auto &cond_lhs : x->conds) {
      if (cond_lhs->lhs->ag_type != ast::SV_AGGRE_NONE) {
        throw InternalError("WHERE 子句中不能用聚集函数作为条件表达式");
      }
    }

    for (auto &sv_sel_col : x->cols) {
      TabCol sel_col = {.tab_name = sv_sel_col->tab_name,
                        .col_name = sv_sel_col->col_name,
                        .ag_type = sv_sel_col->ag_type,
                        .as_name = sv_sel_col->as_name};
      query->cols.push_back(sel_col);
    }

    // 添加group by cols
    for (auto &group_by_col : x->group_by) {
      TabCol col = {.tab_name = group_by_col->tab_name,
                    .col_name = group_by_col->col_name,
                    .ag_type = group_by_col->ag_type,
                    .as_name = group_by_col->as_name};
      query->group_by_cols.push_back(col);
    }

    std::vector<ColMeta> all_cols;
    get_all_cols(query->tables, all_cols);
    if (query->cols.empty()) {
      // select all columns
      for (auto &col : all_cols) {
        TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name, .ag_type = ast::SV_AGGRE_NONE, .as_name = ""};
        query->cols.push_back(sel_col);
      }
    } else {
      // infer table name from column name
      for (auto &sel_col : query->cols) {
        sel_col = check_column(all_cols, sel_col, query->group_by_cols); // 列元数据校验
      }
    }
    // 处理where条件
    get_clause(x->conds, query->conds);
    check_clause(query->tables, query->conds);
  } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {

    // 表名
    query->tables.push_back(x->tab_name);

    // 处理set clause
    for (auto &sv_set_clause : x->set_clauses) {
      auto sv_clause = sv_set_clause.get();
      SetClause set_clause;

      set_clause.lhs = TabCol{.tab_name = x->tab_name, .col_name = sv_clause->col_name};

      auto value = convert_sv_value(sv_clause->val);
      ColMeta col = *(sm_manager_->db_.get_table(x->tab_name).get_col(set_clause.lhs.col_name));

      // char* 和  DATETIME
      if (col.type == TYPE_STRING && value.type == TYPE_DATETIME) {
        Value tmp;
        tmp.set_str(value.datetime_val.encode_to_string());
        value = tmp;
      }

      set_clause.rhs = value;
      set_clause.is_plus_value = sv_clause->is_plus_value;

      query->set_clauses.push_back(set_clause);
    }

    // 处理where条件
    get_clause(x->conds, query->conds);
    check_clause({x->tab_name}, query->conds);

  } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
    // 处理where条件
    get_clause(x->conds, query->conds);
    check_clause({x->tab_name}, query->conds);
  } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
    std::vector<ColMeta> cols;
    get_all_cols({x->tab_name}, cols);
    for (size_t i = 0; i < x->vals.size(); i++) {
      auto value = convert_sv_value(x->vals[i]);
      if (cols[i].type == TYPE_STRING && value.type == TYPE_DATETIME) {
        Value tmp;
        tmp.set_str(value.datetime_val.encode_to_string());
        query->values.push_back(tmp);
      } else {
        query->values.push_back(value);
      }
    }
  } else if (auto x = std::dynamic_pointer_cast<ast::LoadStmt>(parse)) {
    if (!disk_manager_->is_file(x->file_path_)) {
      throw FileNotFoundError(x->file_path_);
    }
    if (!sm_manager_->db_.is_table(x->tab_name_)) {
      throw FileNotFoundError(x->tab_name_);
    }
    query->tables.push_back(x->tab_name_);
    Value tmp;
    tmp.set_str(x->file_path_);
    query->values.push_back(tmp);
  } else {
    // do nothing
  }
  query->parse = std::move(parse);
  return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target, std::vector<TabCol> group_by_cols) {
  if (target.ag_type == ast::SV_AGGRE_COUNT && target.col_name.empty()) {
    // 如果是count(*) 这里的列名是空的 会无法通过check
    for (const auto &col : group_by_cols) {
      if (col.ag_type == ast::SV_AGGRE_NONE) {
        target.col_name = col.col_name;
      }
    }
    if (target.col_name.empty()) {
      target.col_name = all_cols[0].name;
    }
    target.tab_name = all_cols[0].tab_name;
  } else if (target.tab_name.empty()) {
    // Table name not specified, infer table name from column name
    std::string tab_name;
    for (auto &col : all_cols) {
      if (col.name == target.col_name) {
        if (!tab_name.empty()) {
          throw AmbiguousColumnError(target.col_name);
        }
        tab_name = col.tab_name;
      }
    }
    if (tab_name.empty()) {
      throw ColumnNotFoundError(target.col_name);
    }
    target.tab_name = tab_name;
  } else {
    /** TODO: Make sure target column exists */
  }
  return target;
}

TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
  if (target.tab_name.empty()) {
    // Table name not specified, infer table name from column name
    std::string tab_name;
    for (auto &col : all_cols) {
      if (col.name == target.col_name) {
        if (!tab_name.empty()) {
          throw AmbiguousColumnError(target.col_name);
        }
        tab_name = col.tab_name;
      }
    }
    if (tab_name.empty()) {
      throw ColumnNotFoundError(target.col_name);
    }
    target.tab_name = tab_name;
  } else {
    /** TODO: Make sure target column exists */
  }
  return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
  for (auto &sel_tab_name : tab_names) {
    // 这里db_不能写成get_db(), 注意要传指针
    const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
  }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
  conds.clear();
  for (auto &expr : sv_conds) {
    Condition cond;
    cond.lhs_col = {.tab_name = expr->lhs->tab_name,
                    .col_name = expr->lhs->col_name,
                    .ag_type = expr->lhs->ag_type,
                    .as_name = expr->lhs->as_name};
    cond.op = convert_sv_comp_op(expr->op);
    if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
      cond.is_rhs_val = true;
      cond.rhs_val = convert_sv_value(rhs_val);
    } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
      cond.is_rhs_val = false;
      cond.rhs_col = {.tab_name = rhs_col->tab_name,
                      .col_name = rhs_col->col_name,
                      .ag_type = expr->lhs->ag_type,
                      .as_name = expr->lhs->as_name};
    }
    conds.push_back(cond);
  }
}

bool Analyze::comparable(ColType type1, ColType type2) {
  if (type1 == type2) {
    return true;
  }
  if (type1 == TYPE_INT && type2 == TYPE_FLOAT) {
    return true;
  }
  if (type1 == TYPE_FLOAT && type2 == TYPE_INT) {
    return true;
  }
  return false;
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
  // auto all_cols = get_all_cols(tab_names);
  std::vector<ColMeta> all_cols;
  get_all_cols(tab_names, all_cols);
  // Get raw values in where clause
  for (auto &cond : conds) {
    // Infer table name from column name
    cond.lhs_col = check_column(all_cols, cond.lhs_col);
    if (!cond.is_rhs_val) {
      cond.rhs_col = check_column(all_cols, cond.rhs_col);
    }
    TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
    auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
    ColType lhs_type = lhs_col->type;
    ColType rhs_type;
    if (cond.is_rhs_val) {
      if (lhs_col->type == TYPE_STRING && cond.rhs_val.type == TYPE_DATETIME) {
        Value tmp;
        tmp.set_str(cond.rhs_val.datetime_val.encode_to_string());
        cond.rhs_val = tmp;
      }
      // cond.rhs_val.init_raw(lhs_col->len);
      rhs_type = cond.rhs_val.type;
    } else {
      TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
      auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
      rhs_type = rhs_col->type;
    }
    if (!comparable(lhs_type, rhs_type)) {
      std::cout << "analyze check_clause" << std::endl;
      throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
    }
  }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
  Value val;
  if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
    val.set_int(int_lit->val);
  } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
    val.set_float(float_lit->val);
  } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
    val.set_str(str_lit->val);
  } else if (auto datetime_lit = std::dynamic_pointer_cast<ast::DateTimeLit>(sv_val)) {
    val.set_datetime(datetime_lit->val);
  } else {
    throw InternalError("Unexpected sv value type");
  }
  return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
  std::map<ast::SvCompOp, CompOp> m = {
      {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
      {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
  };
  return m.at(op);
}

AggreOp Analyze::convert_sv_aggre_op(ast::SvAggreType type) {
  std::map<ast::SvAggreType, AggreOp> m = {{ast::SV_AGGRE_COUNT, AG_COUNT},
                                           {ast::SV_AGGRE_MAX, AG_MAX},
                                           {ast::SV_AGGRE_MIN, AG_MIN},
                                           {ast::SV_AGGRE_SUM, AG_SUM}};
  return m[type];
}
