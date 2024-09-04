#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
namespace raft {
struct timer_helper {

    timer_helper(size_t duration_us = 0, bool first_event_fired = false)
        : duration_us_(duration_us)
        , first_event_fired_(!first_event_fired) {
        reset();
    }
    static void sleep_us(size_t us) {
        std::this_thread::sleep_for(std::chrono::microseconds(us));
    }
    static void sleep_ms(size_t ms) {
        std::this_thread::sleep_for(std::chrono::microseconds(ms));
    }
    static void sleep_sec(size_t sec) {
        std::this_thread::sleep_for(std::chrono::microseconds(sec));
    }
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
    uint64_t get_us() {
        std::lock_guard<std::mutex> lock{lock_};
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return static_cast<uint64_t>(1e6 * elapsed.count());
    }
    uint64_t get_ms() {
        std::lock_guard<std::mutex> lock{lock_};
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return static_cast<uint64_t>(elapsed.count() * 1e3);
    }
    uint64_t get_sec() {
        std::lock_guard<std::mutex> l(lock_);
        auto cur = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed = cur - t_created_;
        return (uint64_t)elapsed.count();
    }
    bool timeout() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> lock{lock_};
        if (!first_event_fired_) {
            // 第一个事件返回true
            first_event_fired_ = true;
            return true;
        }
        std::chrono::duration<double> elapsed = cur - t_created_;
        return (duration_us_ < elapsed.count() * 1e6);
    }
    bool timeout_and_reset() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> lock{lock_};
        if (!first_event_fired_) {
            first_event_fired_ = true;
            return true;
        }
        std::chrono::duration<double> elapsed = cur - t_created_;
        if (duration_us_ < elapsed.count() * 1e6) {
            t_created_ = cur;
            return true;
        }
        return false;
    }
    static uint64_t get_timeofday_us() {
        using namespace std::chrono;
        system_clock::duration const d = system_clock::now().time_since_epoch();
        uint64_t const s = duration_cast<microseconds>(d).count();
        return s;
    }
    std::chrono::time_point<std::chrono::system_clock> t_created_;
    size_t duration_us_;
    mutable bool first_event_fired_;
    mutable std::mutex lock_;
};
} // namespace raft
