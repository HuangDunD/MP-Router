#include <iostream>

// 假设 unsigned int 是 32 位
static_assert(sizeof(unsigned int) * 8 == 32, "This code assumes unsigned int is 32 bits.");
// 确保我们有一个 64 位的类型可用
static_assert(sizeof(uint64_t) * 8 == 64, "This code requires a 64-bit unsigned integer type (uint64_t).");


class Region {
private:
    unsigned int table_id; // 假设占用 32 位
    unsigned int _inner_region_id; // 假设占用 32 位

public:
    // 构造函数
    Region() : table_id(0), _inner_region_id(0) {
    }

    Region(unsigned int tid, unsigned int irid) : table_id(tid), _inner_region_id(irid) {
    }

    // --- 反序列化: 从组合的 64 位整数构造 ---
    // 通过一个唯一的 64 位 ID 来创建 Region 对象
    explicit Region(uint64_t combined_id) {
        // 提取高 32 位给 table_id
        // 将 combined_id 右移 32 位，高位变成低位，然后转换成 unsigned int
        table_id = static_cast<unsigned int>(combined_id >> 32);

        // 提取低 32 位给 _inner_region_id
        // 直接将 64 位整数转换为 unsigned int 会自动截断，保留低 32 位
        // 或者可以使用位掩码: (combined_id & 0xFFFFFFFFULL)
        _inner_region_id = static_cast<unsigned int>(combined_id);
    }

    // --- 访问器 (Getters) ---
    unsigned int getTableId() const {
        return table_id;
    }

    unsigned int getInnerRegionId() const {
        return _inner_region_id;
    }

    // --- 序列化: 返回唯一的 64 位整数 ---
    // 将对象的状态编码为一个唯一的 64 位无符号整数
    // 这个函数现在确实返回一个数字，而不是写入流
    uint64_t serializeToUint64() const {
        // 将 table_id 转换为 64 位，并左移 32 位，放到高位
        uint64_t high_part = static_cast<uint64_t>(table_id) << 32;

        // 将 _inner_region_id 转换为 64 位 (虽然它只占用低位，转换确保类型匹配)
        uint64_t low_part = static_cast<uint64_t>(_inner_region_id);

        // 使用按位或 (|) 操作合并高位和低位
        return high_part | low_part;
    }

};
