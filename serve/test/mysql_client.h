#pragma once

#include <mysql.h>

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.h"

class MySQLClient {
public:
    explicit MySQLClient(const MySQLConnInfo& info) : info_(info) {
        conn_ = mysql_init(nullptr);
        if (conn_ == nullptr) {
            throw std::runtime_error("mysql_init failed");
        }
        mysql_options(conn_, MYSQL_SET_CHARSET_NAME, "utf8mb4");
        const char* socket = info_.socket.empty() ? nullptr : info_.socket.c_str();
        if (mysql_real_connect(conn_, info_.host.c_str(), info_.user.c_str(),
                               info_.password.c_str(), info_.database.c_str(),
                               info_.port, socket, 0) == nullptr) {
            std::string err = mysql_error(conn_);
            mysql_close(conn_);
            conn_ = nullptr;
            throw std::runtime_error("mysql_real_connect failed: " + err);
        }
    }

    ~MySQLClient() {
        if (conn_ != nullptr) mysql_close(conn_);
    }

    MySQLClient(const MySQLClient&) = delete;
    MySQLClient& operator=(const MySQLClient&) = delete;

    void exec(const std::string& sql) {
        if (mysql_query(conn_, sql.c_str()) != 0) {
            throw std::runtime_error("mysql_query failed: " + std::string(mysql_error(conn_)) +
                                     "; sql=" + sql);
        }
        drain_results();
    }

    uint64_t scalar_uint64(const std::string& sql) {
        if (mysql_query(conn_, sql.c_str()) != 0) {
            throw std::runtime_error("mysql_query failed: " + std::string(mysql_error(conn_)) +
                                     "; sql=" + sql);
        }
        MYSQL_RES* res = mysql_store_result(conn_);
        if (res == nullptr) {
            throw std::runtime_error("mysql_store_result failed: " + std::string(mysql_error(conn_)));
        }
        MYSQL_ROW row = mysql_fetch_row(res);
        uint64_t value = 0;
        if (row != nullptr && row[0] != nullptr) value = std::stoull(row[0]);
        mysql_free_result(res);
        drain_results();
        return value;
    }

private:
    void drain_results() {
        do {
            MYSQL_RES* res = mysql_store_result(conn_);
            if (res != nullptr) mysql_free_result(res);
        } while (mysql_next_result(conn_) == 0);
    }

    MySQLConnInfo info_;
    MYSQL* conn_ = nullptr;
};

inline std::string trim_copy(const std::string& s) {
    size_t begin = 0;
    while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin]))) begin++;
    size_t end = s.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(begin, end - begin);
}

inline MySQLConnInfo parse_mysql_conninfo(const std::string& conninfo) {
    MySQLConnInfo info;
    std::stringstream ss(conninfo);
    std::string token;
    while (ss >> token) {
        auto pos = token.find('=');
        if (pos == std::string::npos) continue;
        std::string key = token.substr(0, pos);
        std::string value = trim_copy(token.substr(pos + 1));
        if (key == "host") info.host = value;
        else if (key == "port") info.port = static_cast<unsigned int>(std::stoul(value));
        else if (key == "user") info.user = value;
        else if (key == "password") info.password = value;
        else if (key == "dbname" || key == "database") info.database = value;
        else if (key == "socket") info.socket = value;
    }
    return info;
}
