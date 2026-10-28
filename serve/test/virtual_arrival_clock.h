#pragma once

#include <cmath>
#include <cstdint>
#include <mutex>
#include <stdexcept>

// A shared measurement timeline, not a rate limiter. Times are monotonic ms.
class VirtualArrivalClock {
public:
    explicit VirtualArrivalClock(double rate = 0) : rate_(rate) {
        if (!std::isfinite(rate) || rate < 0)
            throw std::invalid_argument("virtual arrival rate must be finite and nonnegative");
    }

    double reserve(uint64_t count, double dequeue_time_ms) {
        if (rate_ == 0 || count == 0) return 0;
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) {
            origin_ms_ = dequeue_time_ms;
            started_ = true;
        }
        const double first = origin_ms_ + static_cast<double>(next_) * 1000.0 / rate_;
        next_ += count;
        return first;
    }

    double interval_ms() const { return rate_ > 0 ? 1000.0 / rate_ : 0; }

private:
    const double rate_;
    std::mutex mutex_;
    uint64_t next_ = 0;
    double origin_ms_ = 0;
    bool started_ = false;
};
