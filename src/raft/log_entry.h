#pragma once
#include "raft/basic_types.h"
#include "raft/buffer.h"
#include "raft/log_val_type.h"
#include "raft/ptr.h"
#include "raft/utils.h"
#include <cstdint>
#include <stdexcept>
#include <sys/types.h>
namespace raft {
class log_entry {
public:
  log_entry(ulong term, log_val_type value_type, ptr<buffer> buff, uint64_t timestamp_us)
      : term_(term), value_type_(value_type), buff_(std::move(buff)), timestamp_us_(timestamp_us) {}
  __nocopy__(log_entry);

public:
  ulong get_term() const { return term_; }
  void set_term(ulong term) { term_ = term; }
  log_val_type get_val_type() const { return value_type_; }
  bool is_buf_null() const { return (buff_.get()) ? false : true; }
  buffer &get_buff() const {
    if (!buff_) {
      throw std::runtime_error("get_buf cannot be called for a log_entry"
                               "with nil buffer");
    }
    return *buff_;
  }
  ptr<buffer> get_buf_ptr() const { return buff_; }
  uint64_t get_timestamp() const { return timestamp_us_; }
  void set_timestamp(uint64_t time_stamp) { this->timestamp_us_ = time_stamp; }
  ptr<buffer> serialize() {
    // 清空buff_
    buff_->pos(0);
    ptr<buffer> buf = buffer::alloc(sizeof(ulong) + sizeof(char) + buff_->size());
    buff_->put(term_);
    buff_->put(static_cast<byte>(value_type_));
    buff_->put(*buff_);
    return buf;
  }
  static ptr<log_entry> deserialize(buffer &buf) {
    ulong term = buf.get_ulong();
    auto type = static_cast<log_val_type>(buf.get_byte());
    ptr<buffer> data = buffer::copy(buf);
    return new_ptr<log_entry>(term, data, type);
  }
  static ulong term_in_buffer(buffer &buf) {
    ulong term = buf.get_ulong();
    buf.pos(0); // 重置/清空
    return term;
  }

private:
  ulong term_;
  log_val_type value_type_;
  ptr<buffer> buff_;
  uint64_t timestamp_us_;
};
} // namespace raft
