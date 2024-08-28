#pragma once
#include "execution/execution_abstract.h"
class ProjectionExecutor : public AbstractExecutor {
private:
  std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
  std::vector<ColMeta> cols_;              // 需要投影的字段
  size_t len_;                             // 字段总长度
  std::vector<size_t> sel_idxs_;

public:
  ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols)
      : prev_(std::move(prev)) {

    size_t curr_offset = 0;

    auto &prev_cols = prev_->cols();

    for (auto &sel_col : sel_cols) {
      auto pos = get_col(prev_cols, sel_col);
      sel_idxs_.push_back(pos - prev_cols.begin());
      auto col = *pos;
      col.offset = curr_offset;
      curr_offset += col.len;
      cols_.push_back(col);
    }
    len_ = curr_offset;
  }

  std::string getType() override { return "ProjectionExecutor"; }

  void beginTuple() override { prev_->beginTuple(); }

  void nextTuple() override { prev_->nextTuple(); }

  bool is_end() const override { return prev_->is_end(); }

  std::unique_ptr<RmRecord> Next() override {
    auto record = prev_->Next();
    if (record == nullptr) {
      return nullptr;
    }
    auto new_record = std::make_unique<RmRecord>(len_);
    for (size_t i = 0; i < sel_idxs_.size(); i++) {
      auto &col = cols_[i];
      auto &prev_col = prev_->cols()[sel_idxs_[i]];
      memcpy(new_record->data + col.offset, record->data + prev_col.offset, col.len);
    }

    return new_record;
  }

  const std::vector<ColMeta> &cols() const override { return cols_; }

  Rid &rid() override { return prev_->rid(); }

  size_t tupleLen() const override { return len_; }
};
