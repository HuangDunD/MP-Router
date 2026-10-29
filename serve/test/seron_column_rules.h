#pragma once

#include "seron_range_rules.h"
#include "seron_range_workload.h"
#include "seron_tv_estimator.h"
#include <map>
#include <limits>
#include <string>
#include <utility>
#include <array>
#include <iomanip>
#include <cstdlib>

// Independent per-column histograms. Physical samples rank matching rules;
// they never supply the destination directly. No cross-table alignment is
// inferred merely because two columns have equal names or numeric values.
// This mode implements row-level range assignment, not Seron's complete hybrid
// table/row partitioning, correlation alignment or online rule adaptation.
// Rule ranking follows Sec. 5.1's TV/random-walk pipeline. See estimator header
// for explicit choices where the paper leaves simulation details unspecified.
// For the selected column, score every node by its range-match fraction plus
// existing workload/queue balance weights. Score ties prefer more matches,
// then round-robin. Never vote across different columns.
class SeronColumnRules {
public:
    using Predicate = seron_range::Predicate;
    using PhysicalValue = seron_tv::PhysicalValue;
    struct Selection { int column = -1; double risk = 0.0; };
    static bool diagnostic_did_first_from_env() {
        const char* value = std::getenv("MP_ROUTER_SERON_DIAGNOSTIC_DID_FIRST");
        if (!value || std::string(value) == "0") return false;
        if (std::string(value) == "1") return true;
        throw std::invalid_argument("MP_ROUTER_SERON_DIAGNOSTIC_DID_FIRST must be 0 or 1");
    }
    explicit SeronColumnRules(int nodes, int bins,
                             bool diagnostic_did_first = diagnostic_did_first_from_env(),
                             bool execution_time_weighted = true)
        : nodes_(nodes), bins_(bins), diagnostic_did_first_(diagnostic_did_first),
          execution_time_weighted_(execution_time_weighted) {
        columns_.resize(seron_range::Count);
        for (auto& count : selection_counts_) count.store(0, std::memory_order_relaxed);
    }
    // Initialization only, before router/connector threads start.
    void add_column(size_t id, const seron_range::ColumnSpec& spec,
                    std::vector<PhysicalValue> physical, bool complete = false) {
        auto c = std::make_unique<Column>();
        c->name = spec.table + "." + spec.column;
        c->ranges = std::make_unique<SeronRangeRules>(spec.max_value, nodes_, bins_, execution_time_weighted_);
        c->physical = std::move(physical);
        std::stable_sort(c->physical.begin(), c->physical.end(), [](const auto& a, const auto& b) {
            return a.page != b.page ? a.page < b.page : a.offset < b.offset;
        });
        c->tv = seron_tv::statistics(c->physical, spec.max_value, complete);
        c->complete = complete;
        columns_.at(id) = std::move(c);
    }
    void prepare() {
        if (std::none_of(columns_.begin(), columns_.end(), [](const auto& c) { return bool(c); }))
            throw std::runtime_error("No indexed columns available for Seron range rules");
        publish();
    }
    void observe(const std::vector<Predicate>& predicates, double latency_ms) {
        if (frozen_.load(std::memory_order_acquire)) return;
        std::map<size_t, std::vector<uint64_t>> values;
        for (const auto& p : predicates) if (columns_.at(p.column)) values[p.column].push_back(p.value);
        // Each column observes its predicates independently. RangeRules either
        // counts each key access or divides execution time over this column's keys.
        for (const auto& pair : values) columns_[pair.first]->ranges->observe(pair.second, latency_ms);
    }
    void freeze() {
        for (auto& c : columns_) if (c) c->ranges->freeze();
        publish();
        frozen_.store(true, std::memory_order_release);
    }
    bool frozen() const { return frozen_.load(std::memory_order_acquire); }
    int assign(const std::vector<Predicate>& predicates,
               const std::vector<double>& load_weights = {}, int txn_type = -1,
               Selection* selected = nullptr) const {
        if (selected) *selected = Selection{};
        if (!load_weights.empty() && load_weights.size() != static_cast<size_t>(nodes_))
            throw std::invalid_argument("Seron load-weight dimensions do not match nodes");
        const auto rules = std::atomic_load(&published_);
        if (!rules) throw std::logic_error("Seron column rules not initialized");
        // Lowest estimated conflict rate first; stable column-ID tie-break.
        for (const auto& rule : *rules) {
            std::vector<size_t> matches(nodes_, 0);
            size_t count = 0;
            for (const auto& p : predicates) if (p.column == rule.column) {
                ++matches[destination(rule.boundaries, p.value)]; ++count;
            }
            if (count) {
                if (selected) *selected = Selection{static_cast<int>(rule.column), rule.risk};
                std::vector<int> candidates;
                double best = -std::numeric_limits<double>::infinity();
                size_t best_matches = 0;
                for (int n = 0; n < nodes_; ++n) {
                    const double weight = load_weights.empty() ? 0.0 : load_weights[n];
                    // Reuse the router's workload/queue weights on the same
                    // normalized affinity scale; no additional tuning factor.
                    const double score = static_cast<double>(matches[n]) / count + weight;
                    if (score > best || (score == best && matches[n] > best_matches)) {
                        best = score;
                        best_matches = matches[n];
                        candidates.clear();
                    }
                    if (score == best && matches[n] == best_matches) candidates.push_back(n);
                }
                record_selection(rule.column, txn_type);
                if (candidates.size() == 1) return candidates.front();
                return candidates[tie_cursor_.fetch_add(1, std::memory_order_relaxed) % candidates.size()];
            }
        }
        record_selection(seron_range::Count, txn_type);
        return fallback_.fetch_add(1, std::memory_order_relaxed) % nodes_;
    }
    void print(std::ostream& out) const {
        const auto flags = out.flags();
        const auto precision = out.precision();
        out << std::defaultfloat << std::setprecision(10);
        out << "=== Seron independent column rules (mode 33) ===\n"
            << "  Rule tie policy: " << (diagnostic_did_first_
                ? "DIAGNOSTIC district.d_id first among exactly equal scores (not default)"
                : "default stable column order") << '\n'
            << "  Phase: " << (frozen() ? "frozen rules" : "initial rules") << '\n'
            << "  Ranking: physical-order TV / Monte Carlo page conflict rate (Sec. 5.1)\n"
            << "  Random walk: empirical starts, symmetric +/- step, reflecting value-domain boundaries\n"
            << "  Simulation pages: " << seron_tv::Simulations << ", seed: " << seron_tv::Seed << '\n'
            << "  Page occupancy: mean live rows per occupied sampled page; m rounded, m-1 steps\n"
            << "  Sample minimum: " << seron_tv::MinRows << " rows, " << seron_tv::MinPages
            << " pages, " << seron_tv::MinCrossPagePairs << " adjacent page transitions; full scans exempt\n"
            << "  Score scope: intra-index layout conflicts, not full-transaction write cost\n"
            << "  Assignment: range-match fraction + existing workload/queue balance weights across all nodes; ties prefer more matches, then round-robin\n"
            << "  Cross-table alignment: disabled; no business-semantic pivot\n";
        size_t rank = 0;
        for (const auto& r : *std::atomic_load(&published_)) {
            const auto& c = columns_[r.column];
            out << "  " << c->name << " physical_samples=" << c->physical.size()
                << " conflict_rate=" << r.risk << " rank=" << ++rank << '\n';
            out << "    full_scan=" << c->complete << " sufficient=" << c->tv.sufficient
                << " tv_adjacent_pairs=" << c->tv.adjacent_pairs
                << " tv_cross_page_pairs=" << c->tv.cross_page_pairs
                << " mean_difference=" << c->tv.mean_difference
                << " normalized_step=" << c->tv.normalized_step
                << " mean_records_per_page=" << c->tv.records_per_page << '\n';
            const auto evidence = risk_evidence(*c, r.boundaries);
            out << "    sampled_pages=" << evidence.pages
                << " pair_supported_pages=" << evidence.pair_pages
                << " cross_interval_pages=" << evidence.cross_pages
                << " ordered_pairs=" << evidence.pairs
                << " cross_interval_pairs=" << evidence.cross_pairs
                << " observed_value_min=" << evidence.min_value
                << " observed_value_max=" << evidence.max_value
                << " empirical_pair_rate=" << (evidence.pairs ? evidence.cross_pairs / evidence.pairs : -1)
                << '\n';
            c->ranges->print(out);
        }
        out.flags(flags);
        out.precision(precision);
    }
    void print_selection_counts(std::ostream& out, bool tpcc) const {
        static const char* names[] = {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
        out << "=== Seron selected-rule counts (assignments, not completions) ===\n";
        for (size_t phase = 0; phase < 2; ++phase)
            for (size_t type = 0; type < kTypes; ++type)
                for (size_t col = 0; col <= seron_range::Count; ++col) {
                    const auto count = selection_counts_[count_index(phase, type, col)].load(std::memory_order_relaxed);
                    if (!count) continue;
                    out << "  phase=" << (phase ? "frozen" : "initial") << " txn_type=" << type;
                    if (tpcc && type < 5) out << " txn=" << names[type];
                    out << " rule=" << (col == seron_range::Count ? "fallback" : columns_[col]->name)
                        << " assigned=" << count << '\n';
                }
    }
    std::shared_ptr<const SeronRangeRules::Rule> column_snapshot(size_t id) const {
        return columns_.at(id)->ranges->snapshot();
    }
private:
    struct Column {
        std::string name;
        std::unique_ptr<SeronRangeRules> ranges;
        std::vector<PhysicalValue> physical;
        seron_tv::Statistics tv;
        bool complete = false;
    };
    struct RankedRule { size_t column; std::vector<uint64_t> boundaries; double risk; };
    struct RiskEvidence {
        size_t pages = 0, pair_pages = 0, cross_pages = 0;
        double pairs = 0, same_pairs = 0, cross_pairs = 0;
        uint64_t min_value = 0, max_value = 0;
    };
    static size_t destination(const std::vector<uint64_t>& bounds, uint64_t key) {
        if (key < bounds.front() || key >= bounds.back())
            throw std::out_of_range("Seron predicate outside column domain");
        return std::upper_bound(bounds.begin()+1, bounds.end()-1, key) - (bounds.begin()+1);
    }
    RiskEvidence risk_evidence(const Column& c, const std::vector<uint64_t>& bounds) const {
        RiskEvidence result;
        std::map<uint64_t, std::vector<uint64_t>> pages;
        for (auto v : c.physical) {
            if (result.min_value == 0 || v.value < result.min_value) result.min_value = v.value;
            result.max_value = std::max(result.max_value, v.value);
            auto& counts = pages[v.page];
            if (counts.empty()) counts.resize(nodes_, 0);
            ++counts[destination(bounds, v.value)];
        }
        double pairs = 0, same = 0;
        for (const auto& page : pages) {
            uint64_t total = 0;
            size_t intervals = 0;
            for (auto n : page.second) intervals += n > 0;
            for (auto n : page.second) { total += n; same += double(n) * (n ? n-1 : 0); }
            pairs += double(total) * (total ? total-1 : 0);
            result.pair_pages += total > 1;
            result.cross_pages += intervals > 1;
        }
        result.pages = pages.size();
        result.pairs = pairs;
        result.same_pairs = same;
        result.cross_pairs = pairs - same;
        return result;
    }
    double risk(const Column& c, const std::vector<uint64_t>& bounds) const {
        return seron_tv::estimate(c.physical, c.tv, bounds);
    }
    void publish() {
        auto rules = std::make_shared<std::vector<RankedRule>>();
        for (size_t i = 0; i < columns_.size(); ++i) if (columns_[i]) {
            auto bounds = columns_[i]->ranges->snapshot()->boundaries;
            const double r = risk(*columns_[i], bounds);
            rules->push_back({i, std::move(bounds), r});
        }
        std::stable_sort(rules->begin(), rules->end(), [this](const auto& a, const auto& b) {
            if (a.risk != b.risk) return a.risk < b.risk;
            if (diagnostic_did_first_)
                return a.column == seron_range::DistrictId && b.column != seron_range::DistrictId;
            return false;
        });
        std::shared_ptr<const std::vector<RankedRule>> immutable = rules;
        std::atomic_store(&published_, immutable);
    }
    int nodes_, bins_;
    bool diagnostic_did_first_ = false;
    bool execution_time_weighted_ = true;
    std::vector<std::unique_ptr<Column>> columns_;
    std::shared_ptr<const std::vector<RankedRule>> published_;
    std::atomic<bool> frozen_{false};
    mutable std::atomic<uint64_t> fallback_{0};
    mutable std::atomic<uint64_t> tie_cursor_{0};
    static constexpr size_t kTypes = 8;
    static constexpr size_t count_index(size_t phase, size_t type, size_t column) {
        return (phase * kTypes + type) * (seron_range::Count + 1) + column;
    }
    void record_selection(size_t column, int type) const {
        const size_t slot = type >= 0 && type < 7 ? static_cast<size_t>(type) : 7;
        selection_counts_[count_index(frozen(), slot, column)].fetch_add(1, std::memory_order_relaxed);
    }
    mutable std::array<std::atomic<uint64_t>, 2 * kTypes * (seron_range::Count + 1)> selection_counts_;
};
