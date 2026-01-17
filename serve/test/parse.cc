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

// Helper for Oracle Base64 decoding
int oracle_base64_val(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return 0;
}

// Helper to parse Page ID (Block Number) from Yashan/Oracle Extended ROWID
// Format: OOOOOOFFFBBBBBRRR (18 chars). Block Number is 6 chars at offset 9.
page_id_t parse_yashan_rowid(const std::string& rowid_str) {
    if (rowid_str.length() < 15) return 0; // Too short to be valid extended ROWID
    
    const char* p = rowid_str.c_str() + 9; // Skip Object(6) + File(3) -> Block(6) starts at index 9
    unsigned long long block_id = 0;
    for (int i = 0; i < 6; ++i) {
        block_id = (block_id << 6) + oracle_base64_val(p[i]);
    }
    return (page_id_t)block_id;
}