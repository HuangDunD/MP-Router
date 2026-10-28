#include "tpcc_warehouse_load_order.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <thread>

int main() {
    TPCCWarehouseLoadOrder old(200,10,false,42);
    for (int t=0;t<10;++t) {
        for (int w=t+1;w<=200;w+=10) assert(old.next(t)==w);
        assert(old.next(t)==0);
    }
    TPCCWarehouseLoadOrder a(200,10,true,42), b(200,10,true,42), c(200,10,true,43);
    assert(a.order()==b.order()); assert(a.order()!=c.order());
    auto sorted=a.order();std::sort(sorted.begin(),sorted.end());
    for (int i=0;i<200;++i) assert(sorted[i]==i+1);
    for (int w:a.order()) assert(b.next(0)==w);
    assert(b.next(0)==0);
    std::vector<std::vector<int>> claimed(10);
    std::vector<std::thread> workers;
    for (int t=0;t<10;++t) workers.emplace_back([&,t] {
        for (int w=a.next(t);w;w=a.next(t)) claimed[t].push_back(w);
    });
    for (auto& t:workers)t.join();
    std::vector<int> all;
    for (auto& v:claimed)all.insert(all.end(),v.begin(),v.end());
    std::sort(all.begin(),all.end());assert(all==sorted);
    TPCCWarehouseLoadOrder empty(0,1,true,0), one(1,1,true,UINT64_MAX);
    assert(empty.next(0)==0);assert(one.next(0)==1);assert(one.next(0)==0);
    bool rejected=false;
    try {TPCCWarehouseLoadOrder invalid(-1,1,true,0);}catch(const std::invalid_argument&){rejected=true;}
    assert(rejected);
    std::cout << "Warehouse load order tests passed; seed42 first ten:";
    for (int i=0;i<10;++i)std::cout << ' ' << a.order()[i];
    std::cout << '\n';
}
