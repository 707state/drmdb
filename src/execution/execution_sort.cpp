#include "execution_sort.h"

auto RmCompare::operator()(const std::unique_ptr<RmRecord> &lhs,
                           const std::unique_ptr<RmRecord> &rhs) -> bool {
  for (auto &order_col : order_cols_) {
    auto cols = prev_->cols();
    auto col_meta = *get_col(cols, order_col.tabcol);
    Value left_value = get_record_value(lhs, col_meta);
    Value right_value = get_record_value(rhs, col_meta);
    Condition cond;
    cond.op = OP_GT;
    bool flag = check_cond(left_value, right_value, cond);
    cond.op = OP_EQ;
    bool is_equal = check_cond(left_value, right_value, cond);
    if (is_equal) {
      continue;
    } else if ((!flag && order_col.is_desc) || (flag && !order_col.is_desc)) {
      return false;
    } else {
      return true;
    }
  }
  return true;
}

auto RmCompare::rid() -> Rid & { return _abstract_rid; }

SortExecutor::SortExecutor(std::unique_ptr<AbstractExecutor> prev,
                           std::vector<OrderByCol> order_cols, int limit) {
  prev_ = std::move(prev);
  order_cols_ = order_cols;
  limit_ = limit;
  count_ = 0;
  tuple_num_ = 0;
  used_tuple.clear();
}

auto SortExecutor::getType() -> std::string { return "SortExecutor"; }

void SortExecutor::beginTuple() {
  used_tuple.clear();
  for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
    auto tuple = prev_->Next();
    tuples_.push_back(std::move(tuple));
  }
  std::sort(tuples_.begin(), tuples_.end(), RmCompare(order_cols_, prev_));
  count_ = 0;
}

void SortExecutor::nextTuple() { count_++; }

auto SortExecutor::Next() -> std::unique_ptr<RmRecord> {
  return std::move(tuples_[count_]);
}

auto SortExecutor::cols() const -> const std::vector<ColMeta> & {
  return prev_->cols();
}

auto SortExecutor::is_end() const -> bool {
  if (limit_ != -1 && count_ >= limit_) {
    return true;
  }
  return count_ >= static_cast<int>(tuples_.size());
}

auto SortExecutor::rid() -> Rid & { return _abstract_rid; }
