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
    struct Request {
        uint64_t txn_id = 0;
        int node = -1;
        std::vector<Access> pages;
        uint64_t missing_pages = 0;
        bool delayed = false;
        Clock::time_point first_conflict;
    };
    struct Stats {
        uint64_t admitted = 0;
        uint64_t delayed = 0;
        uint64_t timed_out = 0;
        uint64_t missing_pages = 0;
        uint64_t delay_ns = 0;
        uint64_t active = 0;
    };

    explicit SeronPageConflictTable(std::chrono::microseconds max_delay)
        : max_delay_(max_delay) {
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

        active_.emplace(request.txn_id, Active{request.node, request.pages});
        for (const auto& access : request.pages) {
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
    struct Active { int node; std::vector<Access> pages; };

    bool conflicts(const Request& request) const {
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
        for (const auto& access : txn->second.pages) {
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
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::unordered_map<int, Counts>> pages_;
    std::unordered_map<uint64_t, Active> active_;
    Stats stats_;
};
