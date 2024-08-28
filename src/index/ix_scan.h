#pragma once

#include "ix_defs.h"
#include "ix_index_handle.h"

class IxScan : public RecScan {
  const IxIndexHandle *ih_;
  Iid iid_; // 初始为lower（用于遍历的指针）
  Iid end_; // 初始为upper

public:
  IxScan(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm)
      : ih_(ih), iid_(lower), end_(upper) {}

  void next() override;

  bool is_end() const override { return iid_ == end_; }

  void begin() override {}

  Rid rid() const override;

  const Iid &iid() const { return iid_; }
};
