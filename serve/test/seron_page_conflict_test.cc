#include "seron_page_conflict.h"

#include <atomic>
#include <iostream>
#include <thread>

using Table = SeronPageConflictTable;
using namespace std::chrono_literals;

static void check(bool condition) {
    if (!condition) throw std::runtime_error("Seron conflict-table test failed");
}

static Table::Request request(uint64_t id, int node, std::vector<Table::Access> pages) {
    Table::normalize(pages);
    Table::Request result;
    result.txn_id = id;
    result.node = node;
    result.pages = std::move(pages);
    return result;
}

static void test_conflicts_and_completion() {
    Table table(1000us);
    const auto now = Table::Clock::time_point{};
    auto enqueue = [] { return true; };
    auto read0 = request(1, 0, {{42, false}});
    auto read1 = request(2, 1, {{42, false}});
    auto write1 = request(3, 1, {{42, true}});
    auto other = request(4, 2, {{43, true}});
    check(table.try_dispatch(read0, enqueue, now));
    check(table.try_dispatch(read1, enqueue, now)); // cross-node read/read
    check(!table.try_dispatch(write1, enqueue, now)); // cross-node read/write
    check(table.try_dispatch(other, enqueue, now)); // no head-of-line blocking
    check(table.stats().active == 3); // delayed requests are not active
    table.complete(1);
    check(table.try_dispatch(write1, enqueue, now + 100us)); // same-node read/write
    auto write0 = request(5, 0, {{42, true}});
    auto read2 = request(6, 2, {{42, false}});
    check(!table.try_dispatch(write0, enqueue, now + 100us)); // write/write
    check(!table.try_dispatch(read2, enqueue, now + 100us)); // write/read
    table.complete(3);
    check(table.try_dispatch(read2, enqueue, now + 200us));
    table.complete(2);
    table.complete(6);
    check(table.try_dispatch(write0, enqueue, now + 300us));
    table.complete(4);
    table.complete(5);
    table.complete(5); // duplicate/unknown completion is harmless
    check(table.stats().active == 0);
    check(table.stats().admitted == 6);
    check(table.stats().timed_out == 0);
}

static void test_timeout_and_refcounts() {
    Table table(1000us);
    const auto now = Table::Clock::time_point{};
    auto enqueue = [] { return true; };
    auto a = request(1, 0, {{7, false}, {7, true}, {7, false}});
    check(a.pages.size() == 1 && a.pages[0].write);
    auto b = request(2, 0, {{7, true}});
    auto c = request(3, 1, {{7, false}});
    check(table.try_dispatch(a, enqueue, now));
    check(table.try_dispatch(b, enqueue, now));
    check(!table.try_dispatch(c, enqueue, now));
    table.complete(1);
    check(!table.try_dispatch(c, enqueue, now + 999us)); // b still owns reservation
    check(table.try_dispatch(c, enqueue, now + 1000us));
    check(table.stats().timed_out == 1);
    check(table.stats().delayed == 1);
    check(table.stats().delay_ns == 1000000);
    table.complete(2);
    auto d = request(4, 2, {{7, true}});
    check(!table.try_dispatch(d, enqueue, now + 1000us)); // timeout admission tracked
    table.complete(3);
    check(table.try_dispatch(d, enqueue, now + 1100us));
    table.complete(4);
    check(table.stats().active == 0);

    Table immediate(0us);
    a = request(1, 0, {{7, true}});
    b = request(2, 1, {{7, true}});
    check(immediate.try_dispatch(a, enqueue, now));
    check(immediate.try_dispatch(b, enqueue, now));
    check(immediate.stats().timed_out == 1);
}

static void test_page_identity_and_queue_failure() {
    Table table(1000us);
    const auto now = Table::Clock::time_point{};
    auto a = request(1, 0, {{(uint64_t{1} << 32) | 7, true}});
    auto b = request(2, 1, {{(uint64_t{2} << 32) | 7, true}});
    check(table.try_dispatch(a, [] { return true; }, now));
    check(table.try_dispatch(b, [] { return true; }, now)); // different tables
    auto full = request(3, 2, {{9, true}});
    check(!table.try_dispatch(full, [] { return false; }, now));
    check(table.stats().active == 2 && table.stats().admitted == 2);
    auto next = request(4, 3, {{9, true}});
    check(table.try_dispatch(next, [] { return true; }, now)); // failed push released reservation
    auto throwing = request(5, 4, {{10, true}});
    try {
        table.try_dispatch(throwing, []() -> bool { throw std::runtime_error("enqueue"); }, now);
        check(false);
    } catch (const std::runtime_error&) {}
    check(table.stats().active == 3);
    auto empty = request(6, 0, {});
    empty.missing_pages = 2;
    check(table.try_dispatch(empty, [] { return true; }, now));
    check(table.stats().missing_pages == 2);
    bool duplicate_rejected = false;
    try { table.try_dispatch(empty, [] { return true; }, now); }
    catch (const std::logic_error&) { duplicate_rejected = true; }
    check(duplicate_rejected);
    for (auto id : {1, 2, 4, 6}) table.complete(id);
    check(table.stats().active == 0);
}

static void test_concurrent_admission() {
    Table table(std::chrono::hours(1));
    std::atomic<int> ready{0}, admitted{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&, i] {
            auto candidate = request(i + 1, i, {{100, true}});
            ++ready;
            while (!start.load()) std::this_thread::yield();
            if (table.try_dispatch(candidate, [] { return true; })) ++admitted;
        });
    }
    while (ready.load() != 8) std::this_thread::yield();
    start = true;
    for (auto& thread : threads) thread.join();
    check(admitted == 1); // check-and-register must be atomic
    for (int i = 1; i <= 8; ++i) table.complete(i);
    check(table.stats().active == 0);

    threads.clear();
    std::atomic<int> executing{0}, violations{0};
    for (int node = 0; node < 4; ++node) {
        threads.emplace_back([&, node] {
            for (int i = 0; i < 1000; ++i) {
                const uint64_t id = 100 + node * 1000 + i;
                auto candidate = request(id, node, {{200, true}});
                while (!table.try_dispatch(candidate, [] { return true; })) std::this_thread::yield();
                if (executing.fetch_add(1) != 0) ++violations;
                std::this_thread::yield();
                --executing;
                table.complete(id);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    check(violations == 0 && table.stats().active == 0);
}

int main() {
    test_conflicts_and_completion();
    test_timeout_and_refcounts();
    test_page_identity_and_queue_failure();
    test_concurrent_admission();
    std::cout << "Seron page-conflict tests passed\n";
}
