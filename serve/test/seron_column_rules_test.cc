#include "seron_column_rules.h"
#include <cassert>
#include <iostream>
#include <thread>

int main() {
    using namespace seron_range;
    auto s = specs(100, 4, 3000, 10000);
    SeronColumnRules independent(2, 10);
    independent.add_column(CheckingId, s[CheckingId], {{1,1},{1,2},{2,99},{2,100}});
    independent.add_column(SavingsId, s[SavingsId], {{1,1},{1,100},{2,2},{2,99}});
    independent.prepare();
    independent.observe({{CheckingId,1},{SavingsId,100}}, 100);
    independent.freeze();
    assert(independent.column_snapshot(CheckingId)->boundaries[1] < 20);
    assert(independent.column_snapshot(SavingsId)->boundaries[1] > 80);
    assert(independent.column_snapshot(CheckingId)->latency_ms == 100);
    assert(independent.column_snapshot(SavingsId)->latency_ms == 100);
    // The physical-layout rank, not cross-column majority voting, chooses checking.
    assert(independent.assign({{CheckingId,1},{SavingsId,100},{SavingsId,100}}) == 0);

    SeronColumnRules tpcc_rules(2, 4);
    tpcc_rules.add_column(DistrictWarehouse, s[DistrictWarehouse], {{1,1},{1,4}});
    tpcc_rules.add_column(StockItem, s[StockItem], {{1,1},{1,2},{2,9999},{2,10000}});
    tpcc_rules.prepare();
    // A warehouse predicate cannot override a better-ranked item predicate.
    assert(tpcc_rules.assign({{DistrictWarehouse,1},{StockItem,10000}}) == 1);

    SeronColumnRules balanced(4,10);
    balanced.add_column(CheckingId,s[CheckingId],{});
    balanced.prepare();
    const std::vector<Predicate> tie{{CheckingId,1},{CheckingId,100}};
    assert(balanced.assign(tie,{0.2,1.6,1.6,1.2}) == 3); // only matched nodes eligible
    assert(balanced.assign(tie,{1.2,1.6,1.6,0.2}) == 0);
    assert(balanced.assign({{CheckingId,1}},{0.2,1.6,1.6,1.2}) == 0); // unique match unchanged
    int counts[4] = {};
    for (int i=0;i<100;++i) ++counts[balanced.assign(tie,{1,1,1,1})];
    assert(counts[0] == 50 && counts[3] == 50 && counts[1] == 0 && counts[2] == 0);

    auto has = [](const auto& p, size_t c, uint64_t v) {
        return std::any_of(p.begin(), p.end(), [&](const auto& x) { return x.column == c && x.value == v; });
    };
    auto sb = smallbank(0, std::vector<int>{3,7});
    assert(has(sb, CheckingId,3) && has(sb,SavingsId,3) && has(sb,CheckingId,7));
    auto no = tpcc(0, std::vector<int64_t>{1,2,3,2,99,4,5,88,1,6,1234}, true, false);
    assert(has(no,DistrictWarehouse,1) && has(no,DistrictId,2));
    assert(has(no,StockWarehouse,4) && has(no,StockItem,99) && has(no,ItemId,88));
    assert(!has(no,WarehouseId,1) && !has(no,CustomerId,3));
    auto modern = tpcc(0, std::vector<int64_t>{1,2,3,456,1,99,4,5,1234}, true, true);
    assert(has(modern,StockItem,99) && !has(modern,StockItem,456));
    auto simple = tpcc(0, std::vector<int>{1,2,3,1,99,4,5}, false, false);
    assert(!has(simple,ItemId,99));
    auto pay = tpcc(1,std::vector<int>{1,2,3,100},true,false);
    assert(has(pay,WarehouseId,1) && has(pay,CustomerId,3) && has(pay,CustomerDistrict,2));
    auto status = tpcc(2,std::vector<int>{1,2,3},true,false);
    assert(has(status,OrdersDistrict,2) && has(status,OrderLineWarehouse,1));
    auto delivery = tpcc(3,std::vector<int>{2,7,123},true,false);
    assert(has(delivery,NewOrderDistrict,10) && has(delivery,NewOrderDistrict,1));
    assert(!has(delivery,CustomerId,7)); // carrier ID is not a customer predicate
    auto stock = tpcc(4,std::vector<int>{1,2,15},true,false);
    assert(has(stock,StockWarehouse,1) && !has(stock,StockItem,15));
    for (int type = 0; type <= 6; ++type) assert(!smallbank(type,std::vector<int>{1,2}).empty());

    SeronColumnRules concurrent(2,10);
    concurrent.add_column(CheckingId,s[CheckingId],{{1,1},{1,2}});
    concurrent.prepare();
    std::vector<std::thread> threads;
    for (int n=0; n<4; ++n) threads.emplace_back([&] {
        for (int i=0;i<1000;++i) {
            concurrent.observe({{CheckingId,1}},1);
            assert(concurrent.assign({{CheckingId,1}}) < 2);
        }
    });
    concurrent.freeze();
    for (auto& t:threads) t.join();
    assert(concurrent.frozen());
    std::cout << "Seron per-column rules and SQL-template tests passed\n";
}
