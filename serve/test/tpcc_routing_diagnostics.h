#pragma once
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <tuple>
#include <vector>

// Observation only. Workers own separate plan vectors; a window is published
// after they join. CSV output snapshots completed windows after measurement.
namespace tpcc_diagnostics {
struct TxnIdentity { uint64_t id; int warehouse, type; };
struct NodeChoice {
    int owned, queue, ready, load;
    double affinity;
};
struct Plan {
    TxnIdentity txn;
    size_t step;
    int selected, pages;
    bool reused;
    std::vector<NodeChoice> nodes;
};
struct Window {
    int64_t id = 0;
    size_t transactions = 0;
    // (warehouse, transaction type, final target); -1 targets expose anomalies.
    std::map<std::tuple<int,int,int>, uint64_t> assigned;
    std::vector<std::vector<Plan>> plans;
};
class Collector {
public:
    void append(Window window) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) return;
        windows_.push_back(std::move(window));
    }
    void write(const char* assignments, const char* decisions) {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        std::ofstream a(assignments), d(decisions);
        if (!a || !d) throw std::runtime_error("Cannot open TPC-C diagnostic output");
        a << "window,window_transactions,warehouse,txn_type,node,assigned\n";
        d << "window,partition,step,tx_id,warehouse,txn_type,selected,reused,node,pages,owned,affinity,decision_queue,decision_ready,decision_load\n";
        d << std::setprecision(17);
        for (const auto& w : windows_) {
            for (const auto& x : w.assigned)
                a << w.id << ',' << w.transactions << ',' << std::get<0>(x.first) << ','
                  << std::get<1>(x.first) << ',' << std::get<2>(x.first) << ',' << x.second << '\n';
            for (size_t p=0; p<w.plans.size(); ++p) for (const auto& v : w.plans[p])
                for (size_t n=0; n<v.nodes.size(); ++n) {
                    const auto& s=v.nodes[n];
                    d << w.id << ',' << p << ',' << v.step << ',' << v.txn.id << ','
                      << v.txn.warehouse << ',' << v.txn.type << ',' << v.selected << ','
                      << v.reused << ',' << n << ',' << v.pages << ',' << s.owned << ','
                      << s.affinity << ',' << s.queue << ',' << s.ready << ',' << s.load << '\n';
                }
        }
        if (!a || !d) throw std::runtime_error("Cannot write TPC-C diagnostics");
    }
private:
    std::mutex mutex_;
    bool closed_ = false;
    std::vector<Window> windows_;
};
inline Collector& collector() { static Collector instance; return instance; }
} // namespace tpcc_diagnostics
