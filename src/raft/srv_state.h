#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/buffer_serializer.h"
#include "raft/ptr.h"
#include "raft/utils.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <raft/srv_config.h>
namespace raft {
// 服务器状态
class srv_state {
public:
  srv_state(ulong term, int voted_for, bool et_allowed)
      : term_(term), voted_for_(voted_for), election_timer_allowed_(et_allowed) {}
  using inc_term_func = std::function<ulong(ulong)>;
  __nocopy__(srv_state);

public:
  static ptr<srv_state> deserialize(buffer &buf) {
    if (buf.size() > size_ulong + size_int) {
      return deserialize_v1p(buf);
    }
    return deserialize_v0(buf);
  }

  static ptr<srv_state> deserialize_v0(buffer &buf) {
    ulong term = buf.get_ulong();
    int voted_for = buf.get_int();
    return new_ptr<srv_state>(term, voted_for, true);
  }
  static ptr<srv_state> deserialize_v1p(buffer &buf) {
    buffer_serializer bs(buf);
    auto ver = bs.get_u8();
    (void)ver;
    auto term = bs.get_u64();
    auto voted_for = bs.get_i32();
    bool et_allower = (bs.get_u8() == 1);
    return new_ptr<srv_state>(term, voted_for, et_allower);
  }
  void set_inc_term_func(inc_term_func to) { inc_term_cb_ = to; }
  ulong get_term() const { return term_; }
  void set_term(ulong term) { term_ = term; }
  void inc_term() {
    if (inc_term_cb_) {
      ulong new_term = inc_term_cb_(term_);
      assert(new_term > term_);
      term_ = new_term;
      return;
    }
    term_++;
  }
  int get_voted_for() const { return voted_for_; }
  void set_voted_for(int voted_for) { voted_for_ = voted_for; }
  bool is_election_timer_allowed() const { return election_timer_allowed_; }
  void allow_election_timer(bool to) { election_timer_allowed_ = to; }
  ptr<buffer> serialize() const { return serialize_v1p(CURRENT_VERSION); }
  ptr<buffer> serialize_v0() const {
    ptr<buffer> buf = buffer::alloc(size_ulong + size_int);
    buf->put(term_);
    buf->put(voted_for_);
    buf->put(0);
    return buf;
  }
  ptr<buffer> serialize_v1p(size_t version) const {
    ptr<buffer> buf = buffer::alloc(sizeof(uint8_t) + sizeof(uint64_t) + sizeof(int32_t) + sizeof(uint8_t));
    buffer_serializer bs(buf);
    bs.put_u8(version);
    bs.put_u64(term_);
    bs.put_i32(voted_for_);
    bs.put_u8(election_timer_allowed_ ? 1 : 0);
    return buf;
  }

private:
  const uint8_t CURRENT_VERSION = 1;
  // 任期
  std::atomic<ulong> term_;
  // 投票id编号
  std::atomic<int> voted_for_;
  // 选举的计时
  std::atomic<bool> election_timer_allowed_;
  // 增加任期的回调函数
  std::function<ulong(ulong)> inc_term_cb_;
};
} // namespace raft
