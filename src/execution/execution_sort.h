#pragma once
// 自定义比较类
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
class RmCompare : AbstractExecutor, public ConditionDependedExecutor {
public:
  std::vector<OrderByCol> order_cols_;
  std::unique_ptr<AbstractExecutor> &prev_;

  RmCompare(std::vector<OrderByCol> order_cols, std::unique_ptr<AbstractExecutor> &prev)
      : order_cols_(order_cols), prev_(prev) {}

  auto operator()(const std::unique_ptr<RmRecord> &lhs, const std::unique_ptr<RmRecord> &rhs) -> bool;

  auto rid() -> Rid & override;

  auto Next() -> std::unique_ptr<RmRecord> override { return std::make_unique<RmRecord>(); }
};

class SortExecutor : public AbstractExecutor {
public:
  SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<OrderByCol> order_cols, int limit);

  auto getType() -> std::string override;

  void beginTuple() override;

  void nextTuple() override;

  auto Next() -> std::unique_ptr<RmRecord> override;

  // 实现cols方法，上层需要调用
  auto cols() const -> const std::vector<ColMeta> & override;

  auto is_end() const -> bool override;

  auto rid() -> Rid & override;

private:
  std::unique_ptr<AbstractExecutor> prev_;
  ColMeta cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
  size_t tuple_num_;
  bool is_desc_;
  std::vector<size_t> used_tuple;
  std::unique_ptr<RmRecord> current_tuple;
  std::vector<std::unique_ptr<RmRecord>> tuples_;

  // order_cols，保存多个排序键
  std::vector<OrderByCol> order_cols_;
  // 增加limit_，限制输出元组个数
  int limit_;
  int count_;
};
