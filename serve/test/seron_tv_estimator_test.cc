#include "seron_tv_estimator.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace seron_tv;
    const std::vector<uint64_t> bounds{1, 51, 101};
    std::vector<PhysicalValue> tiny;
    for (uint32_t i=1; i<=38; ++i) tiny.push_back({1, i, i});
    assert(!statistics(tiny,100,false).sufficient);
    assert(statistics(tiny,100,true).sufficient);
    assert(estimate(tiny,statistics(tiny,100,false),bounds)==1);
    std::vector<PhysicalValue> gaps{{1,1,1},{1,2,2},{10,99,1},{10,100,2}};
    auto sampled=statistics(gaps,100,false);
    assert(sampled.adjacent_pairs==2 && sampled.cross_page_pairs==0);
    assert(sampled.mean_difference==1);
    auto full=statistics(gaps,100,true);
    assert(full.adjacent_pairs==3 && full.cross_page_pairs==1);
    assert(full.mean_difference==33);
    auto two_relations=gaps;
    two_relations[2].page=two_relations[3].page=(uint64_t(1)<<32)|1;
    assert(statistics(two_relations,100,true).adjacent_pairs==2);
    std::vector<PhysicalValue> covered;
    for (uint32_t page=1;page<=64;++page)
        for (uint32_t offset=1;offset<=64;++offset) covered.push_back({page,1,offset});
    const auto enough=statistics(covered,100,false);
    assert(enough.sufficient && enough.cross_page_pairs==63);
    assert(estimate(covered,enough,bounds)==0);
    const std::vector<PhysicalValue> alternating{{1,1,1},{1,100,2},{2,1,1},{2,100,2}};
    const auto stat=statistics(alternating,100,true);
    assert(stat.normalized_step==1);
    assert(estimate(alternating,stat,bounds)==1);
    assert(estimate(gaps,full,bounds)==estimate(gaps,full,bounds));
    assert(reflect(-1,99)==1 && reflect(100,99)==98 && reflect(397,99)==1);
    const std::vector<PhysicalValue> singleton{{1,1,1}};
    assert(estimate(singleton,statistics(singleton,1,true),{1,2})==0);
    bool rejected=false;
    try { statistics({{2,1,1},{1,1,1}},100,true); }
    catch (const std::invalid_argument&) { rejected=true; }
    assert(rejected);
    std::cout << "Seron TV / random-walk and coverage tests passed\n";
}
