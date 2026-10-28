// Page-level adaptation of Seron's conflict-delay mechanism (system mode 32).
// Assignment is provided by PAR; this component never changes the target node.
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

class SeronPageConflictTable {
public:
    using Clock = std::chrono::steady_clock;
    struct Access {
        uint64_t table_page;
        bool write;
    };
    // Rule-level adaptation: independent column rules, with no inferred FK or
    // correlation groups. Distinct rules conflict only on overlapping tables.
    struct RuleAccess {
        int rule = -1; // unmatched rule: conservative table-level fallback
        bool high_conflict = false;
        bool write = false;
        std::vector<uint64_t> tables; // sorted, unique
    };
    struct Request {
        uint64_t txn_id = 0;
        int node = -1;
        std::vector<Access> pages;
        uint64_t missing_pages = 0;
        bool delayed = false;
        Clock::time_point first_conflict;
        RuleAccess rule_access;
    };
    struct Stats {
        uint64_t admitted = 0;
        uint64_t delayed = 0;
        uint64_t timed_out = 0;
        uint64_t missing_pages = 0;
        uint64_t delay_ns = 0;
        uint64_t active = 0;
    };

    explicit SeronPageConflictTable(std::chrono::microseconds max_delay,
                                    bool rule_detection = false)
        : max_delay_(max_delay), rule_detection_(rule_detection) {
        if (max_delay.count() < 0) throw std::invalid_argument("negative Seron delay");
    }

    // Merge repeated keys on the same page; a write dominates a read.
    static void normalize(std::vector<Access>& pages) {
        std::sort(pages.begin(), pages.end(), [](const Access& a, const Access& b) {
            return a.table_page < b.table_page;
        });
        size_t count = 0;
        for (const auto& access : pages) {
            if (count != 0 && pages[count - 1].table_page == access.table_page) {
                pages[count - 1].write = pages[count - 1].write || access.write;
            } else {
                pages[count++] = access;
            }
        }
        pages.resize(count);
    }

    // Claim a transaction for a connector, not for queue insertion. The claim
    // callback must be nonblocking and must not call complete synchronously.
    // Explicit time is for deterministic tests; real checks timestamp after locking.
    template <typename Enqueue>
    bool try_dispatch(Request& request, Enqueue enqueue,
                      std::optional<Clock::time_point> check_time = std::nullopt) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = check_time ? *check_time : Clock::now();
        if (active_.count(request.txn_id)) {
            throw std::logic_error("Seron transaction admitted twice");
        }
        const bool conflict = conflicts(request);
        if (conflict && !request.delayed) {
            request.delayed = true;
            request.first_conflict = now;
            ++stats_.delayed;
        }
        const bool timed_out = conflict && now - request.first_conflict >= max_delay_;
        if (conflict && !timed_out) return false;

        active_.emplace(request.txn_id, Active{request.node, request.pages, request.rule_access});
        if (rule_detection_) update_rules(request.rule_access, request.node, true);
        else for (const auto& access : request.pages) {
            auto& counts = pages_[access.table_page][request.node];
            ++counts.total;
            if (access.write) ++counts.writers;
        }
        bool accepted = false;
        try {
            accepted = enqueue();
        } catch (...) {
            erase_active(request.txn_id);
            throw;
        }
        if (!accepted) {
            erase_active(request.txn_id);
            return false;
        }
        ++stats_.admitted;
        stats_.missing_pages += request.missing_pages;
        if (timed_out) ++stats_.timed_out;
        if (request.delayed) {
            stats_.delay_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                now - request.first_conflict).count();
        }
        return true;
    }

    // Called after the final execution attempt, including aborts and router-only
    // consumers, before TIT can reclaim the transaction. Holds no txn pointers.
    void complete(uint64_t txn_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        erase_active(txn_id);
    }

    Stats stats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        Stats result = stats_;
        result.active = active_.size();
        return result;
    }

private:
    struct Counts { uint64_t total = 0; uint64_t writers = 0; };
    struct Active { int node; std::vector<Access> pages; RuleAccess rule_access; };
    struct RuleCounts {
        uint64_t total = 0, writers = 0, high_total = 0, high_writers = 0;
    };

    bool rule_conflicts(const Request& request) const {
        const auto& incoming = request.rule_access;
        for (auto table : incoming.tables) {
            const auto found = rules_.find(table);
            if (found == rules_.end()) continue;
            for (const auto& rule : found->second) for (const auto& node : rule.second) {
                if (node.first == request.node) continue;
                const auto& count = node.second;
                if (!(incoming.write ? count.total : count.writers)) continue;
                // Table fallback, inter-rule conflict, or high-risk intra-rule
                // conflict. No exact page/key overlap or short-query bypass.
                if (incoming.rule < 0 || rule.first < 0 || incoming.rule != rule.first ||
                    incoming.high_conflict ||
                    (incoming.write ? count.high_total : count.high_writers)) return true;
            }
        }
        return false;
    }

    void update_rules(const RuleAccess& access, int node_id, bool add) {
        for (auto table_id : access.tables) {
            if (add) {
                auto& c = rules_[table_id][access.rule][node_id];
                ++c.total;
                if (access.write) ++c.writers;
                if (access.high_conflict) {
                    ++c.high_total;
                    if (access.write) ++c.high_writers;
                }
            } else {
                auto table = rules_.find(table_id);
                auto rule = table->second.find(access.rule);
                auto node = rule->second.find(node_id);
                auto& c = node->second;
                --c.total;
                if (access.write) --c.writers;
                if (access.high_conflict) {
                    --c.high_total;
                    if (access.write) --c.high_writers;
                }
                if (!c.total) rule->second.erase(node);
                if (rule->second.empty()) table->second.erase(rule);
                if (table->second.empty()) rules_.erase(table);
            }
        }
    }

    bool conflicts(const Request& request) const {
        if (rule_detection_) return rule_conflicts(request);
        for (const auto& access : request.pages) {
            const auto page = pages_.find(access.table_page);
            if (page == pages_.end()) continue;
            for (const auto& node : page->second) {
                if (node.first != request.node &&
                    (access.write ? node.second.total != 0 : node.second.writers != 0)) {
                    return true;
                }
            }
        }
        return false;
    }

    void erase_active(uint64_t txn_id) {
        auto txn = active_.find(txn_id);
        if (txn == active_.end()) return;
        if (rule_detection_) update_rules(txn->second.rule_access, txn->second.node, false);
        else for (const auto& access : txn->second.pages) {
            auto page = pages_.find(access.table_page);
            auto node = page->second.find(txn->second.node);
            --node->second.total;
            if (access.write) --node->second.writers;
            if (node->second.total == 0) page->second.erase(node);
            if (page->second.empty()) pages_.erase(page);
        }
        active_.erase(txn);
    }

    std::chrono::microseconds max_delay_;
    bool rule_detection_ = false;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::unordered_map<int, Counts>> pages_;
    std::unordered_map<uint64_t, Active> active_;
    std::unordered_map<uint64_t, std::unordered_map<int,
        std::unordered_map<int, RuleCounts>>> rules_;
    Stats stats_;
};
