#pragma once

#include "seron_range_rules.h"
#include "seron_range_workload.h"
#include <map>
#include <limits>
#include <string>
#include <utility>
#include <array>
#include <iomanip>

// Independent per-column histograms. Physical samples rank matching rules;
// they never supply the destination directly. No cross-table alignment is
// inferred merely because two columns have equal names or numeric values.
// This mode implements row-level range assignment, not Seron's complete hybrid
// table/row partitioning, correlation alignment or online rule adaptation.
// Rank estimation uses observed same-page pairs, not the paper's TV/random walk.
// If a selected column has multiple equality values, use the interval matching
// most values, then the highest existing load-balance weight, then round-robin;
// never vote across different columns or override a unique interval match.
class SeronColumnRules {
public:
    using Predicate = seron_range::Predicate;
    struct PhysicalValue { uint64_t page, value; };
    explicit SeronColumnRules(int nodes, int bins) : nodes_(nodes), bins_(bins) {
        columns_.resize(seron_range::Count);
        for (auto& count : selection_counts_) count.store(0, std::memory_order_relaxed);
    }
    // Initialization only, before router/connector threads start.
    void add_column(size_t id, const seron_range::ColumnSpec& spec,
                    std::vector<PhysicalValue> physical) {
        auto c = std::make_unique<Column>();
        c->name = spec.table + "." + spec.column;
        c->ranges = std::make_unique<SeronRangeRules>(spec.max_value, nodes_, bins_);
        c->physical = std::move(physical);
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
        // Each index-column histogram receives the query latency independently.
        // Multiple predicates on that column divide that weight, not other columns.
        for (const auto& pair : values) columns_[pair.first]->ranges->observe(pair.second, latency_ms);
    }
    void freeze() {
        for (auto& c : columns_) if (c) c->ranges->freeze();
        publish();
        frozen_.store(true, std::memory_order_release);
    }
    bool frozen() const { return frozen_.load(std::memory_order_acquire); }
    int assign(const std::vector<Predicate>& predicates,
               const std::vector<double>& load_weights = {}, int txn_type = -1) const {
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
                const size_t most = *std::max_element(matches.begin(), matches.end());
                std::vector<int> candidates;
                double best = -std::numeric_limits<double>::infinity();
                for (int n = 0; n < nodes_; ++n) if (matches[n] == most) {
                    const double weight = load_weights.empty() ? 0.0 : load_weights[n];
                    if (weight > best) { best = weight; candidates.clear(); }
                    if (weight == best) candidates.push_back(n);
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
            << "  Phase: " << (frozen() ? "frozen rules" : "initial rules") << '\n'
            << "  Ranking: sampled same-page cross-interval pair fraction\n"
            << "  Estimator: implementation proxy, not paper TV/random-walk estimator\n"
            << "  Score scope: this column's sampled pages, not full-transaction write cost\n"
            << "  Zero sampled conflicts do not establish zero population conflicts\n"
            << "  Interval ties: existing workload+queue balance weight, then round-robin\n"
            << "  Cross-table alignment: disabled; no business-semantic pivot\n";
        size_t rank = 0;
        for (const auto& r : *std::atomic_load(&published_)) {
            const auto& c = columns_[r.column];
            out << "  " << c->name << " physical_samples=" << c->physical.size()
                << " conflict_rate=" << r.risk << " rank=" << ++rank << '\n';
            const auto evidence = risk_evidence(*c, r.boundaries);
            out << "    sampled_pages=" << evidence.pages
                << " pair_supported_pages=" << evidence.pair_pages
                << " cross_interval_pages=" << evidence.cross_pages
                << " ordered_pairs=" << evidence.pairs
                << " cross_interval_pairs=" << evidence.cross_pairs
                << " observed_value_min=" << evidence.min_value
                << " observed_value_max=" << evidence.max_value << '\n';
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
        const auto evidence = risk_evidence(c, bounds);
        // Missing physical evidence is not evidence of a conflict-free rule.
        return evidence.pairs > 0 ? 1 - evidence.same_pairs / evidence.pairs : 1.0;
    }
    void publish() {
        auto rules = std::make_shared<std::vector<RankedRule>>();
        for (size_t i = 0; i < columns_.size(); ++i) if (columns_[i]) {
            auto bounds = columns_[i]->ranges->snapshot()->boundaries;
            const double r = risk(*columns_[i], bounds);
            rules->push_back({i, std::move(bounds), r});
        }
        std::stable_sort(rules->begin(), rules->end(), [](const auto& a, const auto& b) { return a.risk < b.risk; });
        std::shared_ptr<const std::vector<RankedRule>> immutable = rules;
        std::atomic_store(&published_, immutable);
    }
    int nodes_, bins_;
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
