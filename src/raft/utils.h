#pragma once
#include "fmt/format.h"
#define __override__ override;
#define __nocopy__(clazz)                                                                                              \
private:                                                                                                               \
  clazz(const clazz &) = delete;                                                                                       \
  clazz &operator=(const clazz &) = delete;

#define __interface_body(clazz)                                                                                        \
public:                                                                                                                \
  clazz() {}                                                                                                           \
  virtual ~clazz() {}                                                                                                  \
  __nocopy__(clazz)

#define auto_lock(lock)                                                                                                \
  std::lock_guard guard { lock }
#define recur_lock(lock)                                                                                               \
  std::unique_lock guard { lock }
#define size_int sizeof(int32)
#define size_ulong sizeof(ulong)
#define size_byte sizeof(byte)

#ifndef ATTR_UNUSED
#if defined(__linux__) || defined(__APPLE__)
#define ATTR_UNUSED __attribute__((unused))
#elif defined(WIN32) || defined(_WIN32)
#define ATTR_UNUSED
#endif
#endif

template <int N> class strfmt {
public:
  explicit strfmt(const char *fmt) : fmt_(fmt) {}

  template <typename... TArgs> std::string fmt(TArgs... args) { return fmt::format(fmt_, args...); }

  strfmt(const strfmt &) = delete;
  strfmt &operator=(const strfmt &) = delete;

private:
  const char *fmt_;
};

using sstrfmt = strfmt<100>;
using lstrfmt = strfmt<200>;
