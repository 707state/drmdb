#pragma once
#include "raft/basic_types.h"
#include "raft/ptr.h"
#include "raft/utils.h"
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
namespace raft {
class buffer {
  buffer() = delete;
  __nocopy__(buffer);

public:
  static ptr<buffer> alloc(const size_t size);
  static ptr<buffer> copy(const buffer &buf);
  static ptr<buffer> clone(const buffer &buf);
  static ptr<buffer> expand(const buffer &buf, uint32_t new_size);
  size_t container_size() const;
  size_t size() const;
  size_t pos() const;
  void pos(size_t p);
  byte *data() const;
  byte *data_begin() const;
  int32 get_int();
  ulong get_ulong();
  byte get_byte();
  const byte *get_bytes(size_t &len);
  void get(ptr<buffer> &dst);
  const char *get_str();
  byte *get_raw(size_t len);
  void put(byte b);
  void put(const char *ba, size_t len);
  void put(const byte *ba, size_t len);
  void put(int32 val);
  void put(ulong val);
  void put(const std::string &str);
  void put(const buffer &buf);
  void put_raw(const byte *ba, size_t len);
};
std::ostream &operator<<(std::ostream &out, buffer &buf);

std::istream &operator>>(std::istream &in, buffer &buf);
} // namespace raft
