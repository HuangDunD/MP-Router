#pragma once
#include <string>
#include <sstream>
#include "common.h"

std::pair<int, int> parse_page_id_from_ctid(const std::string& ctid); 
int64_t decode_hex_key(const std::string &hex_str);