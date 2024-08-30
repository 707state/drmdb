#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/ptr.h"
#include <cstdint>
#include <raft/buffer_serializer.h>
namespace raft {
class cluster_config;
class snapshot {
public:
  enum type : uint8_t {
    raw_binary = 0x1,
    logical_object = 0x2,
  };
  ulong get_last_log_idx() const { return last_log_idx_; }
  ulong get_last_log_term() const { return last_log_term_; }
  ulong size() const { return size_; }
  void set_size(ulong size) { this->size_ = size; }
  type get_type() const { return type_; }
  void set_type(type src) { type_ = src; }
  const ptr<cluster_config> &get_last_config() const { return last_cofig_; }
  static ptr<snapshot> deserialize(buffer &buf);
  static ptr<snapshot> deserialize(buffer_serializer &bs);
  ptr<buffer> serialize();

private:
  ulong last_log_idx_;
  ulong last_log_term_;
  ulong size_;
  ptr<cluster_config> last_cofig_;
  type type_;
};
} // namespace raft
