#pragma once

#include "seron_range_rules.h"
#include "seron_range_workload.h"
#include <map>
#include <limits>
#include <string>
#include <utility>

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
               const std::vector<double>& load_weights = {}) const {
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
                if (candidates.size() == 1) return candidates.front();
                return candidates[tie_cursor_.fetch_add(1, std::memory_order_relaxed) % candidates.size()];
            }
        }
        return fallback_.fetch_add(1, std::memory_order_relaxed) % nodes_;
    }
    void print(std::ostream& out) const {
        out << "=== Seron independent column rules (mode 33) ===\n"
            << "  Ranking: sampled same-page cross-interval pair fraction\n"
            << "  Interval ties: existing workload+queue balance weight, then round-robin\n"
            << "  Cross-table alignment: disabled; no business-semantic pivot\n";
        for (const auto& r : *std::atomic_load(&published_)) {
            const auto& c = columns_[r.column];
            out << "  " << c->name << " physical_samples=" << c->physical.size()
                << " conflict_rate=" << r.risk << "\n";
            c->ranges->print(out);
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
    static size_t destination(const std::vector<uint64_t>& bounds, uint64_t key) {
        if (key < bounds.front() || key >= bounds.back())
            throw std::out_of_range("Seron predicate outside column domain");
        return std::upper_bound(bounds.begin()+1, bounds.end()-1, key) - (bounds.begin()+1);
    }
    double risk(const Column& c, const std::vector<uint64_t>& bounds) const {
        std::map<uint64_t, std::vector<uint64_t>> pages;
        for (auto v : c.physical) {
            auto& counts = pages[v.page];
            if (counts.empty()) counts.resize(nodes_, 0);
            ++counts[destination(bounds, v.value)];
        }
        double pairs = 0, same = 0;
        for (const auto& page : pages) {
            uint64_t total = 0;
            for (auto n : page.second) { total += n; same += double(n) * (n ? n-1 : 0); }
            pairs += double(total) * (total ? total-1 : 0);
        }
        // Missing physical evidence is not evidence of a conflict-free rule.
        return pairs > 0 ? 1 - same / pairs : 1.0;
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
};
