#include "parse.h"

// 解析ctid字符串，提取page_id
// ctid格式为 "(page_id,tuple_index)"，例如 "(0,1)"
std::pair<int, int> parse_page_id_from_ctid(const std::string& ctid) {
    int block = -1, offset = -1;
    sscanf(ctid.c_str(), "(%d,%d)", &block, &offset);
    return {block, offset};
}

int64_t decode_hex_key(const std::string &hex_str) {
    std::stringstream ss;
    std::string clean_hex;

    for (char c : hex_str) {
        if (std::isxdigit(c)) clean_hex += c;
    }

    // 解析为 64-bit 小端整数
    if (clean_hex.size() < 16) return -1;

    int64_t value = 0;
    for (int i = 0; i < 8; i++) {
        std::string byte_str = clean_hex.substr(i * 2, 2);
        uint8_t byte_val = std::stoi(byte_str, nullptr, 16);
        value |= (static_cast<int64_t>(byte_val) << (8 * i));  // Little-endian shift
    }

    return value;
}