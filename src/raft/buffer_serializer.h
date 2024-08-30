#pragma once
#include "raft/basic_types.h"
#include "raft/ptr.h"
#include "raft/utils.h"
#include <cstddef>
#include <cstdint>
#include <string>
namespace raft {
class buffer;
class buffer_serializer {
  friend buffer;

public:
  enum endiances {
    LITTLE = 0x0,
    BIG = 0x1,
  };
  explicit buffer_serializer(buffer &src_buf, endiances endian = LITTLE);
  explicit buffer_serializer(ptr<buffer> &src_buf_ptr, endiances endian = LITTLE);
  __nocopy__(buffer_serializer);

public:
  // 获取cursor位置
  inline size_t pos() const { return pos_; }
  size_t size() const;
  // 设置cursor位置
  void pos(size_t new_pos);
  void *data() const;         // 获取从当前的position开始的指针
  void put_u8(uint8_t val);   // put 1-byte unsigned integer
  void put_u16(uint16_t val); // put 2-byte unsigned integer
  void put_u32(uint32_t val);
  void put_u64(uint64_t val);
  void put_i8(int8_t val);
  void put_i16(int16_t val);
  void put_i32(int32_t val);
  void put_i64(int64_t);
  void put_raw(const void *raw_ptr, size_t len);
  void put_buffer(const buffer &buf);
  void put_bytes(const void *raw_ptr, size_t len);
  void put_str(const std::string &str);
  void put_cstr(const char *str);
  uint8_t get_u8();
  uint16_t get_u16();
  uint32_t get_u32();
  uint64_t get_u64();
  int8_t get_i8();
  int16_t get_i16();
  int32_t get_i32();
  int64_t get_i64();
  void *get_raw(size_t len);
  void get_buffer(ptr<buffer> &dst);
  void *get_bytes(size_t &len);
  std::string get_str();
  const char *get_cstr();

private:
  bool is_valid(size_t len) const;
  endiances endian_;
  buffer &buf_;
  size_t pos_;
};
} // namespace raft
