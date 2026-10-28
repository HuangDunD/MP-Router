#include "tpcc_routing_diagnostics.h"
#include <cassert>
#include <cstdio>
#include <iterator>
#include <string>
#include <thread>

int main() {
    tpcc_diagnostics::Collector collector;
    auto add = [&](int id) {
        tpcc_diagnostics::Window w;
        w.id=id; w.transactions=3;
        w.assigned[{7,0,1}]=2;
        w.assigned[{7,1,0}]=1;
        w.plans.resize(1);
        w.plans[0].push_back({{123,7,0},0,1,3,true,
            {{2,4,1,5,2.0/3.0},{1,2,1,3,1.0/3.0}}});
        collector.append(std::move(w));
    };
    std::thread a(add,1), b(add,2); a.join(); b.join();
    const char* assignments="/tmp/tpcc_diagnostics_test_assignments.csv";
    const char* decisions="/tmp/tpcc_diagnostics_test_decisions.csv";
    collector.write(assignments,decisions);
    std::ifstream af(assignments), df(decisions);
    std::string av((std::istreambuf_iterator<char>(af)),{});
    std::string dv((std::istreambuf_iterator<char>(df)),{});
    assert(av.find("1,3,7,0,1,2\n")!=std::string::npos);
    assert(av.find("2,3,7,1,0,1\n")!=std::string::npos);
    assert(dv.find("1,0,0,123,7,0,1,1,0,3,2,")!=std::string::npos);
    assert(dv.find("2,0,0,123,7,0,1,1,1,3,1,")!=std::string::npos);
    std::remove(assignments); std::remove(decisions);
}
