#pragma once
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>
namespace raft {
struct timer_helper {

  timer_helper(size_t duration_us = 0, bool first_event_fired = false)
      : duration_us_(duration_us), first_event_fired_(!first_event_fired) {
    reset();
  }
  static void sleep_us(size_t us) { std::this_thread::sleep_for(std::chrono::microseconds(us)); }
  static void sleep_ms(size_t ms) { std::this_thread::sleep_for(std::chrono::microseconds(ms)); }
  static void sleep_sec(size_t sec) { std::this_thread::sleep_for(std::chrono::microseconds(sec)); }
  void reset() {
    std::lock_guard<std::mutex> lock{lock_};
    t_created_ = std::chrono::system_clock::now();
  }
  size_t get_duration_us() const {
    std::lock_guard lock{lock_};
    return duration_us_;
  }
  void set_duration_us(size_t us) {
    std::lock_guard lock{lock_};
    this->duration_us_ = us;
  }

  void set_duration_ms(size_t ms) { set_duration_us(ms * 1000); }

  void set_duration_sec(size_t sec) { set_duration_us(sec * 1000000); }
  std::chrono::time_point<std::chrono::system_clock> t_created_;
  size_t duration_us_;
  mutable bool first_event_fired_;
  mutable std::mutex lock_;
};
} // namespace raft
