#pragma once
#include <atomic>
#include <cstdint>
#include <numeric>
#include <random>
#include <stdexcept>
#include <vector>

// Only assigns whole warehouse loading tasks. Never changes IDs or row order.
class TPCCWarehouseLoadOrder {
public:
    TPCCWarehouseLoadOrder(int warehouses, int workers, bool shuffled, uint64_t seed)
        : warehouses_(warehouses), workers_(workers), shuffled_(shuffled) {
        if (warehouses < 0 || workers <= 0) throw std::invalid_argument("Invalid warehouse load dimensions");
        order_.resize(warehouses);
        std::iota(order_.begin(), order_.end(), 1);
        legacy_next_.resize(workers);
        std::iota(legacy_next_.begin(), legacy_next_.end(), 1);
        if (shuffled) {
            // Explicit Fisher-Yates + rejection sampling: stable across C++ libraries.
            std::mt19937_64 rng(seed);
            for (size_t i = order_.size(); i > 1; --i) {
                const uint64_t bound = i;
                const uint64_t threshold = (uint64_t(0) - bound) % bound;
                uint64_t value;
                do { value = rng(); } while (value < threshold);
                std::swap(order_[i-1], order_[value % bound]);
            }
        }
    }
    int next(int worker) {
        if (worker < 0 || worker >= workers_) throw std::out_of_range("Invalid load worker");
        if (shuffled_) {
            const size_t pos = cursor_.fetch_add(1, std::memory_order_relaxed);
            return pos < order_.size() ? order_[pos] : 0;
        }
        // One caller per worker, matching the existing strided loop exactly.
        const int warehouse = legacy_next_[worker];
        if (warehouse > warehouses_) return 0;
        legacy_next_[worker] += workers_;
        return warehouse;
    }
    const std::vector<int>& order() const { return order_; }
private:
    int warehouses_, workers_;
    bool shuffled_;
    std::vector<int> order_, legacy_next_;
    std::atomic<size_t> cursor_{0};
};
