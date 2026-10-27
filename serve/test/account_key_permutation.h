#pragma once
#include <cstdint>
#include <stdexcept>

// Seeded bijection on account IDs [1, count]. Six-round balanced Feistel on
// the smallest even-bit domain, followed by cycle walking. No modulo-hash
// collisions and no O(account_count) permutation array. Not cryptographic.
class AccountKeyPermutation {
public:
    AccountKeyPermutation(uint64_t count, uint64_t seed) : count_(count), seed_(seed) {
        if (!count || count > UINT32_MAX) throw std::invalid_argument("Invalid account permutation domain");
        unsigned bits = 0;
        for (uint64_t x = count - 1; x; x >>= 1) ++bits;
        half_ = (bits + 1) / 2;
        mask_ = (uint64_t{1} << half_) - 1;
    }
    uint64_t operator()(uint64_t key) const {
        if (!key || key > count_) throw std::out_of_range("Account outside permutation domain");
        if (count_ == 1) return 1;
        uint64_t x = key - 1;
        do {
            uint64_t left = x >> half_, right = x & mask_;
            for (uint64_t round = 0; round < 6; ++round) {
                const uint64_t next = left ^ (mix(right ^ mix(seed_ + round)) & mask_);
                left = right; right = next;
            }
            x = (left << half_) | right;
        } while (x >= count_);
        return x + 1;
    }
private:
    static uint64_t mix(uint64_t x) {
        x += UINT64_C(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
        return x ^ (x >> 31);
    }
    uint64_t count_, seed_, mask_;
    unsigned half_;
};
