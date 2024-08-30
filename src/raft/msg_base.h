#pragma once
#include "raft/msg_type.h"
#include <cstdint>
#include <sys/types.h>
namespace raft {
class msg_base {
public:
  msg_base(ulong term, msg_type type, int32_t src, int32_t dst) : term_(term), type_(type), src_(src), dst_(dst) {}
  virtual ~msg_base() = default;
  [[nodiscard]] auto get_term() const -> ulong { return term_; }
  [[nodiscard]] auto get_type() const -> msg_type { return type_; }
  [[nodiscard]] auto get_src() const -> int32_t { return src_; }
  [[nodiscard]] auto get_dst() const -> int32_t { return dst_; }

private:
  // 任期
  ulong term_;
  msg_type type_;
  // 发送节点的编号
  int32_t src_;
  // 接受节点的编号
  int32_t dst_;
};
} // namespace raft
