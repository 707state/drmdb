#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
class DeleteExecutor : public AbstractExecutor, public ConditionDependedExecutor {
public:
  DeleteExecutor(std::shared_ptr<SmManager> sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                 std::vector<Rid> rids, std::shared_ptr<Context> context);

  void beginTuple() override {
    if (context_ != nullptr) {
      context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
    }
  }

  auto getType() -> std::string override;

  auto cols() const -> const std::vector<ColMeta> & override;

  auto Next() -> std::unique_ptr<RmRecord> override;

  auto rid() -> Rid & override;

private:
  TabMeta tab_;                  // 表的元数据
  std::vector<Condition> conds_; // delete的条件
  RmFileHandle *fh_;             // 表的数据文件句柄
  std::vector<Rid> rids_;        // 需要删除的记录的位置
  std::string tab_name_;         // 表名称
};
