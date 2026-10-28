#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <set>
#include <stdexcept>
#include <vector>

// Seron Sec. 5.1: physical-order TV -> normalized mean step -> page random walks.
// The paper does not specify direction/boundary handling or the RNG seed.
// Reproduction choices: symmetric +/- steps, reflecting boundaries, empirical
// row-value starting distribution, 10,000 simulated pages, fixed seed 33.
namespace seron_tv {
constexpr size_t MinRows = 4096;
constexpr size_t MinPages = 32;
constexpr size_t MinCrossPagePairs = 16;
constexpr size_t FullScanRowLimit = 65536;
constexpr size_t Simulations = 10000;
constexpr uint64_t Seed = 33;

struct PhysicalValue {
    uint64_t page, value;
    uint32_t offset = 0;
};
struct Statistics {
    size_t rows = 0, pages = 0, adjacent_pairs = 0, cross_page_pairs = 0;
    double mean_difference = 0, normalized_step = 0, records_per_page = 0;
    bool sufficient = false;
};

inline Statistics statistics(const std::vector<PhysicalValue>& rows,
                             uint64_t max_value, bool complete) {
    Statistics s;
    s.rows = rows.size();
    std::set<uint64_t> pages;
    long double tv = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto& r = rows[i];
        if (!r.value || r.value > max_value) throw std::invalid_argument("TV value outside column domain");
        pages.insert(r.page);
        if (!i) continue;
        const auto& p = rows[i-1];
        if (r.page < p.page || (r.page == p.page && r.offset < p.offset))
            throw std::invalid_argument("TV samples must be in physical tuple order");
        const bool same_relation = (r.page >> 32) == (p.page >> 32);
        // Do not interpret jumps over unsampled blocks as adjacent records.
        if (same_relation && (complete || r.page == p.page || r.page == p.page + 1)) {
            tv += r.value >= p.value ? r.value - p.value : p.value - r.value;
            ++s.adjacent_pairs;
            s.cross_page_pairs += r.page != p.page;
        }
    }
    s.pages = pages.size();
    s.records_per_page = s.pages ? double(s.rows) / s.pages : 0;
    s.mean_difference = s.adjacent_pairs ? double(tv / s.adjacent_pairs) : 0;
    s.normalized_step = max_value > 1 ? s.mean_difference / (max_value - 1) : 0;
    // Full scans of genuinely small tables are valid even below these targets.
    s.sufficient = s.rows > 0 && (complete ||
        (s.rows >= MinRows && s.pages >= MinPages && s.cross_page_pairs >= MinCrossPagePairs));
    return s;
}

inline uint64_t bounded(std::mt19937_64& rng, uint64_t n) {
    const uint64_t threshold = (uint64_t(0) - n) % n;
    uint64_t x;
    do { x = rng(); } while (x < threshold);
    return x % n;
}
inline double reflect(double x, double extent) {
    if (extent <= 0) return 0;
    x = std::fmod(x, 2 * extent);
    if (x < 0) x += 2 * extent;
    return x <= extent ? x : 2 * extent - x;
}

inline double estimate(const std::vector<PhysicalValue>& rows, const Statistics& s,
                       const std::vector<uint64_t>& boundaries) {
    if (!s.sufficient) return 1.0; // unknown, not an observed 100% conflict rate
    if (boundaries.size() < 2 || boundaries.front() != 1)
        throw std::invalid_argument("Invalid TV rule boundaries");
    const size_t m = std::max<size_t>(1, std::llround(s.records_per_page));
    if (m < 2 || boundaries.size() == 2) return 0;
    const double extent = double(boundaries.back() - 2); // values in [1, max_value]
    std::mt19937_64 rng(Seed);
    long double rates = 0;
    std::vector<uint64_t> counts(boundaries.size()-1);
    for (size_t trial = 0; trial < Simulations; ++trial) {
        std::fill(counts.begin(), counts.end(), 0);
        double x = double(rows[bounded(rng, rows.size())].value - 1);
        for (size_t j = 0; j < m; ++j) {
            const auto node = std::upper_bound(boundaries.begin()+1, boundaries.end()-1, x+1)
                - (boundaries.begin()+1);
            ++counts[node];
            if (j+1 < m) x = reflect(x + ((rng() & 1) ? s.mean_difference : -s.mean_difference), extent);
        }
        long double same = 0;
        for (auto n : counts) same += static_cast<long double>(n) * (n ? n-1 : 0);
        rates += 1 - same / (static_cast<long double>(m) * (m-1));
    }
    return double(rates / Simulations);
}
} // namespace seron_tv
