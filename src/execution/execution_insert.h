#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
class InsertExecutor : public AbstractExecutor, public ConditionDependedExecutor {
public:
  InsertExecutor(std::shared_ptr<SmManager> sm_manager, const std::string &tab_name, std::vector<Value> values,
                 std::shared_ptr<Context> context);

  void beginTuple() override {
    if (context_ != nullptr) {
      context_->lock_mgr_->lock_IX_on_table(context_->txn_.get(), fh_->GetFd());
    }
  }

  auto getType() -> std::string override;

  auto Next() -> std::unique_ptr<RmRecord> override;

  auto rid() -> Rid & override;

private:
  TabMeta tab_;               // 表的元数据
  std::vector<Value> values_; // 需要插入的数据
  RmFileHandle *fh_;          // 表的数据文件句柄
  Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
};
