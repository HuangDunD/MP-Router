#include "seron_range_rules.h"
#include "seron_pending_queue.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <thread>

int main() {
    SeronRangeRules uniform(100, 4, 10);
    assert(uniform.assign({1}) == 0 && uniform.assign({25}) == 0);
    assert(uniform.assign({26}) == 1 && uniform.assign({100}) == 3);
    assert(uniform.assign({80, 2}) == 0); // fixed tie-break, not access order
    assert(uniform.assign({80, 80, 2}) == 3);
    for (uint64_t k = 1; k <= 100; ++k) uniform.observe({k}, 1);
    uniform.freeze();
    assert((uniform.snapshot()->boundaries == std::vector<uint64_t>{1, 26, 51, 76, 101}));
    assert(uniform.snapshot()->samples == 100 && uniform.snapshot()->latency_ms == 100);
    uniform.observe({1}, 999);
    uniform.freeze();
    assert(uniform.snapshot()->samples == 100); // frozen after warmup

    SeronRangeRules skew(100, 2, 10);
    skew.observe({1}, 90); skew.observe({100}, 10); skew.freeze();
    assert(skew.snapshot()->boundaries[1] < 11); // smaller hot interval

    SeronRangeRules aligned(100, 2, 10);
    aligned.observe({1, 1, 80}, 30); aligned.freeze();
    assert(aligned.snapshot()->latency_ms == 30); // no double-counted txn latency
    assert(aligned.assign({1, 1}) == aligned.assign({1})); // aligned tables

    SeronRangeRules empty(2, 4);
    empty.freeze(); assert(!empty.snapshot()->learned);
    assert(empty.assign({1}) >= 0 && empty.assign({2}) < 4);
    bool threw = false;
    try { empty.assign({0}); } catch (const std::out_of_range&) { threw = true; }
    assert(threw);

    SeronRangeRules concurrent(200, 4);
    std::vector<std::thread> workers;
    for (int n = 0; n < 4; ++n) workers.emplace_back([&] {
        for (int i = 0; i < 1000; ++i) {
            concurrent.observe({1 + static_cast<uint64_t>(i % 200)}, 1);
            assert(concurrent.assign({200}) < 4);
        }
    });
    for (auto& worker : workers) worker.join();
    assert(!concurrent.frozen()); concurrent.freeze();
    assert(concurrent.snapshot()->samples == 4000);
    workers.clear();
    SeronRangeRules publication(200, 4);
    for (int n = 0; n < 4; ++n) workers.emplace_back([&] {
        for (int i = 0; i < 1000; ++i) {
            publication.observe({100}, 1);
            assert(publication.assign({100}) < 4);
        }
    });
    publication.freeze();
    for (auto& worker : workers) worker.join();

    // Range assignment does not alter the existing page-level delay boundary.
    SeronPageConflictTable conflicts(std::chrono::seconds(10));
    SeronPendingQueue<int> q0, q1;
    SeronPageConflictTable::Request a, b;
    a.txn_id = 1; a.node = uniform.assign({1}); a.pages = {{42, true}};
    b.txn_id = 2; b.node = uniform.assign({100}); b.pages = {{42, true}};
    q0.push({1, a}); q1.push({2, b});
    assert(q0.pop_ready(conflicts, 20).size() == 1);
    assert(q1.pop_ready(conflicts, 20).empty());
    conflicts.complete(1);
    assert(q1.pop_ready(conflicts, 20).size() == 1);
    conflicts.complete(2);
    std::ostringstream report; concurrent.print(report);
    assert(report.str().find("Warmup samples: 4000") != std::string::npos);
    std::cout << "Seron range-rule tests passed\n";
}
