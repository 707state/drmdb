#pragma once
#include <string>

#include <cstdarg>

static inline std::string msg_if_given(const char *format, ...) {
  if (format[0] == 0x0) {
    return "";
  } else {
    size_t len = 0;
    char msg[2048];
    va_list args;
    va_start(args, format);
    len = vsnprintf(msg, 2048, format, args);
    va_end(args);

    // Get rid of newline at the end.
    if (msg[len - 1] == '\n') {
      len--;
      msg[len] = 0x0;
    }
    return std::string(msg, len);
  }
}

#define L_TRACE (6)
#define L_DEBUG (5)
#define L_INFO (4)
#define L_WARN (3)
#define L_ERROR (2)
#define L_FATAL (1)

#define p_lv(lv, ...)                                                                                                  \
  if (logger_ && logger_->get_level() >= (lv))                                                                         \
  logger_->put_details((lv), __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// trace.
#define p_tr(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 6)                                                                            \
  logger_->put_details(6, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// debug verbose.
#define p_dv(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 5)                                                                            \
  logger_->put_details(5, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// debug.
#define p_db(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 5)                                                                            \
  logger_->put_details(5, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// info.
#define p_in(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 4)                                                                            \
  logger_->put_details(4, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// warning.
#define p_wn(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 3)                                                                            \
  logger_->put_details(3, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// error.
#define p_er(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 2)                                                                            \
  logger_->put_details(2, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// fatal.
#define p_ft(...)                                                                                                      \
  if (logger_ && logger_->get_level() >= 1)                                                                            \
  logger_->put_details(1, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))
