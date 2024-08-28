#pragma once
#include "common/context.h"
#include "common/defs.h"
#include "system/sm_meta.h"
class AbstractExecutor {
public:
  Rid _abstract_rid;

  std::shared_ptr<Context> context_;

  virtual ~AbstractExecutor() = default;

  virtual auto tupleLen() const -> size_t { return 0; };

  virtual auto cols() const -> const std::vector<ColMeta> & {
    std::vector<ColMeta> *_cols = nullptr;
    return *_cols;
  }

  virtual auto getType() -> std::string { return "AbstractExecutor"; };

  virtual void beginTuple() {};

  virtual void nextTuple() {};

  virtual auto is_end() const -> bool { return true; }

  virtual auto rid() -> Rid & = 0;

  virtual auto Next() -> std::unique_ptr<RmRecord> = 0;

  virtual auto get_col_offset(const TabCol &target) -> ColMeta { return ColMeta(); };

  auto get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) -> std::vector<ColMeta>::const_iterator {
    auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
      return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return pos;
  }
};
