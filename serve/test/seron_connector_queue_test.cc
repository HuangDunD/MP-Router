#include <cassert>
#include <future>
#include <iostream>
#include "txn_queue.h"

std::atomic<int> g_chimera_phase{0};
std::atomic<uint64_t> g_chimera_phase1_exec_cnt{0}, g_chimera_phase2_exec_cnt{0};
using namespace std::chrono_literals;

static void check(bool value) {
    if (!value) throw std::runtime_error("Seron connector-queue test failed");
}

static TxnQueue::SeronEntry entry(TxnQueueEntry& txn, int node, uint64_t page) {
    SeronPageConflictTable::Request request;
    request.txn_id = txn.tx_id;
    request.node = node;
    request.pages = {{page, true}};
    return {&txn, std::move(request)};
}

static void test_queue_boundary_and_bypass() {
    auto table = std::make_shared<SeronPageConflictTable>(1h);
    TxnQueue q0(nullptr, nullptr, nullptr, nullptr, 0, 8);
    TxnQueue q1(nullptr, nullptr, nullptr, nullptr, 1, 8);
    q0.set_seron_conflicts(table); q1.set_seron_conflicts(table);
    TxnQueueEntry a(1, 0), b(2, 0), c(3, 0), d(4, 0);
    std::vector<TxnQueue::SeronEntry> v0{entry(a, 0, 42), entry(d, 0, 44)};
    std::vector<TxnQueue::SeronEntry> v1{entry(b, 1, 42), entry(c, 1, 43)};
    check(q0.push_seron_txns(v0) && q1.push_seron_txns(v1));
    check(table->stats().active == 0); // Queue insertion never reserves pages.
    int call, type; double wait;
    auto ready = q0.pop_txn(&call, &type, &wait);
    check(ready.size() == 2 && type == 2 && table->stats().active == 2);
    check(ready.front()->combine_txn_count == 2);
    ready = q1.pop_txn(&call, &type, &wait);
    check(ready.size() == 1 && ready[0] == &c && q1.size() == 1);
    check(table->stats().active == 3 && table->stats().delayed == 1);
    q1.set_batch_finished(); // A blocked entry is not an exhausted batch.
    auto consumer = std::async(std::launch::async, [&] { return q1.pop_txn(); });
    check(consumer.wait_for(10ms) == std::future_status::timeout);
    table->complete(a.tx_id); q1.notify_seron_completion();
    check(consumer.wait_for(1s) == std::future_status::ready);
    ready = consumer.get();
    check(ready.size() == 1 && ready[0] == &b);
    check(q1.pop_txn().empty());
    for (auto id : {b.tx_id, c.tx_id, d.tx_id}) table->complete(id);
    check(table->stats().active == 0);
}

static void test_timeout_and_batch_limit() {
    auto table = std::make_shared<SeronPageConflictTable>(1ms);
    TxnQueue q0(nullptr, nullptr, nullptr, nullptr, 0, 8);
    TxnQueue q1(nullptr, nullptr, nullptr, nullptr, 1, 8);
    q0.set_seron_conflicts(table); q1.set_seron_conflicts(table);
    TxnQueueEntry a(10, 0), b(11, 0), c(12, 0), d(13, 0);
    std::vector<TxnQueue::SeronEntry> v{entry(a, 0, 42), entry(c, 0, 43), entry(d, 0, 44)};
    check(q0.push_seron_txns(v));
    auto ready = q0.pop_txn();
    check(ready.size() == 2 && q0.size() == 1 && table->stats().active == 2);
    std::vector<TxnQueue::SeronEntry> other{entry(b, 1, 42)};
    check(q1.push_seron_txns(other));
    ready = q1.pop_txn();
    check(ready.size() == 1 && table->stats().timed_out == 1);
    check(table->stats().delay_ns >= 1000000);
    table->complete(a.tx_id); table->complete(b.tx_id); table->complete(c.tx_id);
    ready = q0.pop_txn(); check(ready.size() == 1);
    table->complete(d.tx_id); check(table->stats().active == 0);
}

static void test_backpressure_and_shutdown() {
    auto table = std::make_shared<SeronPageConflictTable>(1h);
    TxnQueue q(nullptr, nullptr, nullptr, nullptr, 0, 2);
    q.set_seron_conflicts(table);
    TxnQueueEntry a(20, 0), b(21, 0), c(22, 0);
    std::vector<TxnQueue::SeronEntry> first{entry(a, 0, 1), entry(b, 0, 2)};
    check(q.push_seron_txns(first));
    auto producer = std::async(std::launch::async, [&] {
        std::vector<TxnQueue::SeronEntry> more{entry(c, 0, 3)};
        return q.push_seron_txns(more);
    });
    check(producer.wait_for(10ms) == std::future_status::timeout);
    check(table->stats().active == 0);
    q.force_stop();
    check(producer.wait_for(1s) == std::future_status::ready && !producer.get());
    check(q.pop_txn().empty() && table->stats().active == 0);

    TxnQueue q1(nullptr, nullptr, nullptr, nullptr, 1, 2);
    q1.set_seron_conflicts(table);
    auto blocker = entry(a, 0, 1).request;
    check(table->try_dispatch(blocker, [] { return true; }));
    std::vector<TxnQueue::SeronEntry> blocked{entry(b, 1, 1)};
    check(q1.push_seron_txns(blocked));
    auto consumer = std::async(std::launch::async, [&] { return q1.pop_txn(); });
    check(consumer.wait_for(10ms) == std::future_status::timeout);
    q1.force_stop();
    check(consumer.wait_for(1s) == std::future_status::ready && consumer.get().empty());
    check(table->stats().active == 1); table->complete(a.tx_id);
}

static void test_parallel_connectors() {
    auto table = std::make_shared<SeronPageConflictTable>(1h);
    std::vector<std::unique_ptr<TxnQueue>> queues;
    std::vector<std::unique_ptr<TxnQueueEntry>> txns;
    for (int node = 0; node < 4; ++node) {
        queues.push_back(std::make_unique<TxnQueue>(nullptr, nullptr, nullptr, nullptr, node, 100));
        queues.back()->set_seron_conflicts(table);
        std::vector<TxnQueue::SeronEntry> batch;
        for (int i = 0; i < 100; ++i) {
            txns.push_back(std::make_unique<TxnQueueEntry>(1000 + node * 100 + i, 0));
            batch.push_back(entry(*txns.back(), node, 99));
        }
        check(queues.back()->push_seron_txns(batch));
        queues.back()->set_finished();
    }
    check(table->stats().active == 0);
    std::atomic<int> completed{0}, violations{0};
    std::mutex executing_mutex;
    int executing[4]{};
    std::vector<std::thread> workers;
    for (int node = 0; node < 4; ++node) {
        for (int worker = 0; worker < 2; ++worker) {
            workers.emplace_back([&, node] {
                while (true) {
                    auto ready = queues[node]->pop_txn();
                    if (ready.empty()) break;
                    for (auto* txn : ready) {
                        {
                            std::lock_guard<std::mutex> lock(executing_mutex);
                            for (int other = 0; other < 4; ++other)
                                if (other != node && executing[other] != 0) ++violations;
                            ++executing[node];
                        }
                        std::this_thread::yield();
                        {
                            std::lock_guard<std::mutex> lock(executing_mutex);
                            --executing[node];
                        }
                        table->complete(txn->tx_id);
                        for (auto& queue : queues) queue->notify_seron_completion();
                        ++completed;
                    }
                }
            });
        }
    }
    for (auto& worker : workers) worker.join();
    check(completed == 400 && violations == 0);
    check(table->stats().active == 0 && table->stats().timed_out == 0);
}

int main() {
    SYSTEM_MODE = 32;
    BatchExecutorPOPTxnSize = 2;
    test_queue_boundary_and_bypass();
    test_timeout_and_batch_limit();
    test_backpressure_and_shutdown();
    test_parallel_connectors();
    std::cout << "Seron connector-queue tests passed\n";
}
