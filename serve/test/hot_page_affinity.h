#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

// Optional mode11 scoring state. Integer page counts remain the source of truth
// for feasibility/barriers; weights affect only the ownership affinity score.
class HotPageAffinityState {
public:
    static double page_weight(bool has_rw_conflict, uint32_t distinct_transactions) {
        return has_rw_conflict && distinct_transactions > 1 ? distinct_transactions : 1.0;
    }

    void reset(std::vector<double> weights, size_t node_count) {
        weights_ = std::move(weights);
        owned_weights_.assign(node_count, 0.0);
        total_weight_ = 0.0;
        for (double weight : weights_) {
            assert(weight >= 1.0);
            total_weight_ += weight;
        }
    }

    bool enabled() const { return !owned_weights_.empty(); }

    // Called only when the ownership mode satisfies this access (read/write).
    // Apply both initial snapshots and subsequent planned ownership changes.
    void adjust(std::vector<int>& counts, size_t page_index, size_t owner, int delta) {
        counts[owner] += delta;
        if (enabled()) owned_weights_[owner] += delta * weights_[page_index];
    }

    double affinity(size_t owner) const {
        return total_weight_ > 0.0 ? owned_weights_[owner] / total_weight_ : 0.0;
    }

private:
    std::vector<double> weights_;
    std::vector<double> owned_weights_;
    double total_weight_ = 0.0;
};
