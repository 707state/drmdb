#pragma once
#include <common/errors.h>
#include <cstring>
#include <optional>
static constexpr int BITMAP_WIDTH = 8;
static constexpr unsigned BITMAP_HIGHEST_BIT = 0x80u; // 128 (2^7)

class Bitmap {
public:
  static void init(char *bm, int size) { memset(bm, 0, size); }
  static void set(char *bm, int pos) { bm[get_bucket(pos)] |= get_bit(pos); }
  static void reset(char *bm, int pos) { bm[get_bucket(pos)] &= static_cast<char>(~get_bit(pos)); }
  static auto is_set(const char *bm, int pos) -> bool { return (bm[get_bucket(pos)] & get_bit(pos)) != 0; }
  static std::optional<int> next_bit(bool bit, const char *bm, int max_n, int curr) {
    for (int i = curr + 1; i < max_n; i++) {
      if (is_set(bm, i) == bit) {
        return i;
      }
    }
    return std::nullopt;
  }
  static int first_bit(bool bit, const char *bm, int max_n) {
    auto ans = next_bit(bit, bm, max_n, -1);
    if (ans == std::nullopt) [[unlikely]] {
      throw InternalError("No enough bits.");
    }
    return ans.value();
  }

private:
  static int get_bucket(int pos) { return pos / BITMAP_WIDTH; }
  static char get_bit(int pos) { return BITMAP_HIGHEST_BIT >> static_cast<char>(pos % BITMAP_WIDTH); }
};
