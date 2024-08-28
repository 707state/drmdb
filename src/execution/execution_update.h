#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
class UpdateExecutor : public AbstractExecutor, public ConditionDependedExecutor {
private:
  TabMeta tab_;
  std::vector<Condition> conds_;
  RmFileHandle *fh_;
  std::vector<Rid> rids_;
  std::vector<SetClause> set_clauses_;

public:
  UpdateExecutor(std::shared_ptr<SmManager> sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                 std::vector<Condition> conds, std::vector<Rid> rids, std::shared_ptr<Context> context);

  void beginTuple() override {
    if (context_ != nullptr) {
      context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
    }
  };

  auto getType() -> std::string override;

  auto cols() const -> const std::vector<ColMeta> & override;

  /**
   * 重写Next方法，用于迭代并更新记录。
   * @return 返回一个unique_ptr指向更新后的记录。
   */
  auto Next() -> std::unique_ptr<RmRecord> override;

  auto rid() -> Rid & override;
};
