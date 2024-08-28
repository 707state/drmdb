#pragma once
#include "execution/execution_abstract.h"
#include "execution/execution_conddep.h"
class NestedLoopJoinExecutor : public AbstractExecutor, public ConditionDependedExecutor {
private:
  std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
  std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
  size_t len_;                              // join后获得的每条记录的长度
  std::vector<ColMeta> cols_;               // join后获得的记录的字段

  std::vector<Condition> fed_conds_; // join条件
  bool isend;

public:
  NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                         std::vector<Condition> conds)
      : left_(std::move(left)), right_(std::move(right)), len_(left_->tupleLen() + right_->tupleLen()),
        cols_(left_->cols()), fed_conds_(std::move(conds)), isend(false) {

    auto right_cols = right_->cols();
    for (auto &col : right_cols) {
      col.offset += left_->tupleLen();
    }

    cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
  }

  std::string getType() override { return "NestedLoopJoinExecutor"; }

  const std::vector<ColMeta> &cols() const override { return cols_; }

  void beginTuple() override {
    left_->beginTuple();
    right_->beginTuple();

    isend = false;
  }

  void nextTuple() override {
    while (true) {
      left_->nextTuple();

      if (left_->is_end()) {
        right_->nextTuple();
        left_->beginTuple();
        if (right_->is_end()) {
          isend = true;
          return;
        }
      }

      auto lrec = left_->Next();
      auto rrec = right_->Next();

      bool flag = true;

      for (auto &cond : fed_conds_) {

        // 从左表中查出此字段信息
        int left_off = 0, left_len = 0;
        ColType left_type;

        auto col_meta = left_.get()->cols();
        for (auto &lcol : col_meta) {
          if (lcol.name.compare(cond.lhs_col.col_name) == 0) {
            left_off = lcol.offset;
            left_len = lcol.len;
            left_type = lcol.type;
          }
        }

        int right_off = 0, right_len = 0;
        ColType right_type;

        col_meta = right_.get()->cols();
        for (auto &rcol : col_meta) {
          if (rcol.name.compare(cond.rhs_col.col_name) == 0) {
            right_off = rcol.offset;
            right_len = rcol.len;
            right_type = rcol.type;
          }
        }

        Value left_val = get_record_value(*lrec.get(), left_off, left_len, left_type);
        Value right_val = get_record_value(*rrec.get(), right_off, right_len, right_type);

        if (!check_cond(left_val, right_val, cond)) {
          flag = false;
          break;
        }
      }

      if (flag) {
        // 符合条件，返回
        return;
      }
    }
  }

  bool is_end() const override {
    bool end = right_->is_end();
    return end;
  }

  std::unique_ptr<RmRecord> Next() override {

    RmRecord new_rec(len_);

    if (is_end()) {
      return nullptr;
    }

    auto lrec = left_->Next();
    auto rrec = right_->Next();

    for (auto &col : cols_) {

      // 将左右转换成ConditionDependedExecutor

      if (col.offset < left_->tupleLen()) {
        // 左表
        // 查找此列在左表中的位置
        auto col_meta = left_.get()->cols();
        for (auto &c : col_meta) {
          if (c.name.compare(col.name) == 0) {
            new_rec.set_column_value(col.offset, col.len, lrec->get_column_value(c.offset, c.len));
            break;
          }
        }

      } else {
        // 右表
        // 查找此列在右表中的位置
        auto col_meta = right_.get()->cols();
        for (auto &c : col_meta) {
          if (c.name.compare(col.name) == 0) {

            new_rec.set_column_value(col.offset, col.len, rrec->get_column_value(c.offset, c.len));
            break;
          }
        }
      }
    }

    auto unique_ptr_new_rec = std::make_unique<RmRecord>(new_rec);

    return unique_ptr_new_rec;
  }

  Rid &rid() override { return _abstract_rid; }
};
