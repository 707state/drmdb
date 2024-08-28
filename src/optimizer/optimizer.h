#pragma once
#include "system/sm_manager.h"
#include <optimizer/plan.h>
#include <optimizer/planner.h>
class Optimizer {
private:
  std::shared_ptr<SmManager> sm_manager_;
  std::shared_ptr<Planner> planner_;

public:
  Optimizer(std::shared_ptr<SmManager> sm_manager, std::shared_ptr<Planner> planner)
      : sm_manager_(sm_manager), planner_(planner) {}

  std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
    if (auto x = std::dynamic_pointer_cast<ast::Help>(query->parse)) {
      // help;
      return std::make_shared<OtherPlan>(T_Help, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->parse)) {
      // show tables;
      return std::make_shared<OtherPlan>(T_ShowTable, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->parse)) {
      // desc table;
      return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowIndex>(query->parse)) {
      // show index;
      return std::make_shared<OtherPlan>(T_ShowIndex, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->parse)) {
      // begin;
      return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->parse)) {
      // abort;
      return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->parse)) {
      // commit;
      return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->parse)) {
      // rollback;
      return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::SetOutputFileOff>(query->parse)) {
      return std::make_shared<OtherPlan>(T_SetOutputFileOff, std::string());
    } else {
      return planner_->do_planner(query, context);
    }
  }
};
