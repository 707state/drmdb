#pragma once
#include <utility>

#include "analyze/analyze.h"
#include "optimizer/plan.h"
#include "system/sm_manager.h"
class Planner {
private:
  std::shared_ptr<SmManager> sm_manager_;

public:
  explicit Planner(std::shared_ptr<SmManager> sm_manager) : sm_manager_(std::move(sm_manager)) {}

  std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

private:
  std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
  std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

  std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

  std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

  std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

  // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
  // bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>&
  // index_col_names);

  std::pair<bool, IndexMeta> get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                                            std::vector<std::string> &index_col_names);

  ColType interp_sv_type(ast::SvType sv_type) {
    std::map<ast::SvType, ColType> m = {{ast::SV_TYPE_INT, TYPE_INT},
                                        {ast::SV_TYPE_FLOAT, TYPE_FLOAT},
                                        {ast::SV_TYPE_STRING, TYPE_STRING},
                                        {ast::SV_TYPE_DATETIME, TYPE_DATETIME}};
    return m.at(sv_type);
  }
};
