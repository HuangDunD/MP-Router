#include "account_key_permutation.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>

int main() {
    for (uint64_t n : {1,2,3,4,5,15,16,17,200,1000,100003}) {
        for (uint64_t seed : {0,42,12345}) {
            AccountKeyPermutation p(n,seed), same(n,seed);
            std::vector<bool> seen(n+1,false);
            for (uint64_t k=1;k<=n;++k) {
                auto v=p(k);
                assert(v>=1 && v<=n && !seen[v] && v==same(k));
                seen[v]=true;
            }
        }
    }
    AccountKeyPermutation large(10000000,42), other(10000000,43);
    // TPC-C: one bijection applies to home and remote warehouse IDs.
    // It preserves all local/remote relationships and the 50-warehouse hot set size.
    AccountKeyPermutation warehouses(200,42);
    std::vector<bool> hot(201,false);
    int hot_bins[4] = {};
    for (uint64_t w=1; w<=200; ++w) {
        if (w<=50) {
            assert(!hot[warehouses(w)]);
            hot[warehouses(w)] = true;
            ++hot_bins[(warehouses(w)-1)/50];
        }
        for (uint64_t remote=1; remote<=200; ++remote)
            assert((w==remote) == (warehouses(w)==warehouses(remote)));
    }
    for (int count : hot_bins) assert(count>0);
    int bins[4]={}, changed=0;
    for (uint64_t k=1;k<=10000;++k) {
        auto v=large(k); assert(v>=1 && v<=10000000);
        ++bins[(v-1)/2500000]; changed += v != other(k);
    }
    for (int n : bins) assert(n>2000 && n<3000);
    assert(changed>9900);
    // Relabeling preserves equality, distinctness and the unordered co-access set.
    std::vector<uint64_t> before{1,77,1,900}, after;
    for (auto k : before) after.push_back(large(k));
    assert(after[0]==after[2] && after[0]!=after[1]);
    AccountKeyPermutation max(UINT32_MAX,42);
    assert(max(UINT32_MAX)>=1 && max(UINT32_MAX)<=UINT32_MAX);
    bool bad=false;
    try { large(0); } catch (const std::out_of_range&) { bad=true; }
    assert(bad);
    std::cout << "Account-key permutation tests passed\n";
}
