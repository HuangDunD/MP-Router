#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <stdexcept>
#include <vector>

// Mode 33: a shared, aligned key-range rule learned from completed warmup
// transactions. Each transaction contributes its DB execution time once,
// divided equally over its predicate occurrences. No page IDs enter this rule.
class SeronRangeRules {
public:
    using Key = uint64_t;
    struct Rule {
        std::vector<Key> boundaries; // [start, end), one interval per primary
        bool learned = false;
        uint64_t samples = 0;
        double latency_ms = 0;
    };

    SeronRangeRules(Key key_count, int nodes, int bins = 64)
        : key_count_(key_count), nodes_(nodes) {
        if (key_count == 0 || key_count == UINT64_MAX || nodes <= 0 || bins <= 0)
            throw std::invalid_argument("Invalid Seron range-rule dimensions");
        const size_t count = static_cast<size_t>(std::min<Key>(key_count, bins));
        weights_.assign(count, 0.0);
        for (size_t i = 0; i <= count; ++i)
            edges_.push_back(1 + static_cast<Key>((static_cast<long double>(key_count) * i) / count));
        auto rule = std::make_shared<Rule>();
        for (int n = 0; n <= nodes_; ++n)
            rule->boundaries.push_back(1 + static_cast<Key>((static_cast<long double>(key_count) * n) / nodes_));
        rules_ = rule;
    }

    bool frozen() const { return frozen_.load(std::memory_order_acquire); }

    void observe(const std::vector<Key>& keys, double latency_ms) {
        if (frozen() || keys.empty() || !std::isfinite(latency_ms) || latency_ms <= 0) return;
        for (auto key : keys) validate(key);
        std::lock_guard<std::mutex> lock(mutex_);
        if (frozen()) return;
        const double share = latency_ms / keys.size();
        for (auto key : keys) {
            const auto bin = std::upper_bound(edges_.begin(), edges_.end(), key) - edges_.begin() - 1;
            weights_[bin] += share;
        }
        ++samples_;
        latency_ms_ += latency_ms;
    }

    // Publish an immutable rule once. Routing threads can continue concurrently;
    // every individual routing decision sees either the old or the new rule.
    void freeze() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (frozen()) return;
        auto rule = std::make_shared<Rule>(*snapshot());
        rule->samples = samples_;
        rule->latency_ms = latency_ms_;
        const double total = std::accumulate(weights_.begin(), weights_.end(), 0.0);
        if (total > 0) {
            rule->learned = true;
            size_t bin = 0;
            double before = 0;
            for (int n = 1; n < nodes_; ++n) {
                const double target = total * n / nodes_;
                while (bin + 1 < weights_.size() && before + weights_[bin] < target) {
                    before += weights_[bin++];
                }
                // Uniform density within a histogram bin; never split a key.
                const double fraction = weights_[bin] > 0 ?
                    std::clamp((target - before) / weights_[bin], 0.0, 1.0) : 0;
                const Key offset = static_cast<Key>(std::ceil(
                    fraction * static_cast<double>(edges_[bin + 1] - edges_[bin])));
                rule->boundaries[n] = std::clamp(edges_[bin] + offset,
                    rule->boundaries[n - 1], key_count_ + 1);
            }
        }
        std::shared_ptr<const Rule> published = rule;
        std::atomic_store_explicit(&rules_, published, std::memory_order_release);
        frozen_.store(true, std::memory_order_release);
    }

    int assign(const std::vector<Key>& keys) const {
        if (keys.empty()) throw std::invalid_argument("Empty Seron routing predicates");
        const auto rule = snapshot();
        std::vector<size_t> matches(nodes_, 0);
        for (auto key : keys) {
            validate(key);
            const auto node = std::upper_bound(rule->boundaries.begin() + 1,
                rule->boundaries.end() - 1, key) - (rule->boundaries.begin() + 1);
            ++matches[node];
        }
        // Stable tie-break: lowest primary ID. No ownership/load-based override.
        return static_cast<int>(std::max_element(matches.begin(), matches.end()) - matches.begin());
    }

    std::shared_ptr<const Rule> snapshot() const {
        return std::atomic_load_explicit(&rules_, std::memory_order_acquire);
    }

    void print(std::ostream& out) const {
        const auto rule = snapshot();
        out << "=== Seron range rules (mode 33) ===\n"
            << "  Histogram bins: " << weights_.size() << "\n"
            << "  Warmup samples: " << rule->samples << "\n"
            << "  Warmup DB execution weight (ms): " << rule->latency_ms << "\n"
            << "  Rule source: " << (rule->learned ? "latency-weighted warmup" : "equal-width fallback (no samples)") << "\n";
        for (int n = 0; n < nodes_; ++n)
            out << "  N" << n << ": [" << rule->boundaries[n] << ", "
                << rule->boundaries[n + 1] << ")\n";
    }

private:
    void validate(Key key) const {
        if (key < 1 || key > key_count_) throw std::out_of_range("Seron key outside rule domain");
    }
    Key key_count_;
    int nodes_;
    std::vector<Key> edges_;
    std::vector<double> weights_;
    std::shared_ptr<const Rule> rules_;
    std::atomic<bool> frozen_{false};
    std::mutex mutex_;
    uint64_t samples_ = 0;
    double latency_ms_ = 0;
};
