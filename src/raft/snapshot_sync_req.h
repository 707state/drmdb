#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/buffer_serializer.h"
#include "raft/ptr.h"
#include "raft/utils.h"
namespace raft {
class snapshot;

class snapshot_sync_req {
public:
  snapshot_sync_req(ptr<snapshot> snapshot, ulong offset, ptr<buffer> data, bool done)
      : snapshot_(std::move(snapshot)), offset_(offset), data_(std::move(data)), done_(done) {}
  __nocopy__(snapshot_sync_req);

public:
  static ptr<snapshot_sync_req> deserialize(buffer &buf);
  static ptr<snapshot_sync_req> deserialize(buffer_serializer &bs);
  snapshot &get_snapshot() const { return *snapshot_; }
  ulong get_offset() const { return offset_; }
  void set_offset(ulong offset) { offset_ = offset; }
  buffer &get_data() const { return *data_; }
  bool is_done() const { return done_; }
  ptr<buffer> serialize();

private:
  ptr<snapshot> snapshot_;
  ulong offset_;
  ptr<buffer> data_;
  bool done_;
};
} // namespace raft
