#include "hot_page_affinity.h"

#include <cmath>
#include <iostream>

static void near(double actual, double expected) {
    assert(std::abs(actual - expected) < 1e-12);
}

int main() {
    // Off: retain original integer ownership counts without allocating weights.
    HotPageAffinityState off;
    std::vector<int> counts(2, 0);
    assert(!off.enabled());
    off.adjust(counts, 0, 0, 1);
    off.adjust(counts, 1, 1, 1);
    off.adjust(counts, 0, 0, -1);
    assert((counts == std::vector<int>{0, 1}));

    assert(HotPageAffinityState::page_weight(true, 8) == 8.0);
    assert(HotPageAffinityState::page_weight(false, 1000) == 1.0); // read-read
    assert(HotPageAffinityState::page_weight(true, 1) == 1.0); // private page

    // One hot page on node0 outweighs two cold pages on node1, but integer
    // counts still report only one vs two pages (neither node is feasible).
    HotPageAffinityState hot;
    hot.reset({8.0, 1.0, 1.0}, 2);
    counts.assign(2, 0);
    hot.adjust(counts, 0, 0, 1);
    hot.adjust(counts, 1, 1, 1);
    hot.adjust(counts, 2, 1, 1);
    assert((counts == std::vector<int>{1, 2}));
    near(hot.affinity(0), 0.8);
    near(hot.affinity(1), 0.2);

    // Planned transfer: remove the old compatible ownership, then add the new.
    hot.adjust(counts, 0, 0, -1);
    hot.adjust(counts, 0, 1, 1);
    assert((counts == std::vector<int>{0, 3}));
    near(hot.affinity(0), 0.0);
    near(hot.affinity(1), 1.0);

    // Unit weights exactly match the original affinity, including partial
    // knowledge: the third access stays in the denominator with no owner.
    hot.reset({1.0, 1.0, 1.0}, 2);
    counts.assign(2, 0);
    hot.adjust(counts, 0, 0, 1);
    hot.adjust(counts, 1, 1, 1);
    for (size_t n = 0; n < counts.size(); ++n)
        near(hot.affinity(n), static_cast<double>(counts[n]) / 3.0);

    // Shared ownership satisfying a read can contribute to multiple nodes.
    hot.reset({8.0, 1.0}, 2);
    counts.assign(2, 0);
    hot.adjust(counts, 0, 0, 1);
    hot.adjust(counts, 0, 1, 1);
    near(hot.affinity(0), 8.0 / 9.0);
    near(hot.affinity(1), 8.0 / 9.0);
    hot.reset({}, 2);
    near(hot.affinity(0), 0.0);

    std::cout << "hot_page_affinity tests passed\n";
}
