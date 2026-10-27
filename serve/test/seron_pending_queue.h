#pragma once

#include <list>
#include <utility>
#include <vector>
#include "seron_page_conflict.h"

// Caller holds the per-primary queue mutex. The shared conflict table makes
// check-and-register atomic across primaries. Merely queued entries are inactive.
template <typename Txn>
class SeronPendingQueue {
public:
    struct Entry {
        Txn txn;
        SeronPageConflictTable::Request request;
    };

    void push(Entry entry) { pending_.push_back(std::move(entry)); }
    bool empty() const { return pending_.empty(); }
    size_t size() const { return pending_.size(); }

    std::vector<Txn> pop_ready(SeronPageConflictTable& conflicts, size_t limit) {
        std::vector<Txn> ready;
        ready.reserve(limit);
        for (auto it = pending_.begin(); it != pending_.end() && ready.size() < limit;) {
            if (conflicts.try_dispatch(it->request, [&] {
                    ready.push_back(it->txn);
                    return true;
                })) {
                it = pending_.erase(it);
            } else {
                ++it; // A blocked transaction must not block executable followers.
            }
        }
        return ready;
    }

private:
    std::list<Entry> pending_;
};
