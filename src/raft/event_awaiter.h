#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
/*
@ description: 用于进行两个线程的同步
*/
class EventAwaiter {
public:
  enum class AS {
    idle = 0x0,
    ready = 0x1,
    waiting = 0x2,
    done = 0x3,
  };

public:
  EventAwaiter() : status(AS::idle) {}
  void reset() { status.store(AS::idle); }
  void wait() { wait_us(0); }
  void wait_ms(size_t time_ms) { wait_us(time_ms * 1000); }
  // 让当前事件等待，同时支持超时时间
  void wait_us(size_t time_us) {
    AS expected = AS::idle;
    // 从AS::idle转换到AS::ready
    if (status.compare_exchange_strong(expected, AS::ready)) {
      std::unique_lock<std::mutex> l{cv_lock};
      expected = AS::ready;
      // 从AS::ready转换到AS::waiting
      if (status.compare_exchange_strong(expected, AS::waiting)) {
        if (time_us) { // 如果设置了time_us, 就等待time_us或超时
          cv.wait_for(l, std::chrono::microseconds(time_us));
        } else {
          cv.wait(l);
        }
        status.store(AS::done);
      } else {
        // cv_lock已经被invoke获取
      }
    } else {
      // invoke在此之前已经被调用
    }
  }
  void invoke() {
    AS expected = AS::idle;
    if (status.compare_exchange_strong(expected, AS::done)) {
      // wait()没有被调用，什么都不需要做
      return;
    }
    std::unique_lock l{cv_lock};
    expected = AS::ready;
    if (status.compare_exchange_strong(expected, AS::done)) {
      // wait()在invoke()前调用
      // 但是cv_lock被invoke()获取

    } else {
      // 处于waiting
      cv.notify_all();
    }
  }

private:
  std::atomic<AS> status;
  std::mutex cv_lock;
  std::condition_variable cv;
};
