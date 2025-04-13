// author: huangdund, year: 2025
#pragma once
#include <iostream>
#include <pqxx/pqxx> // PostgreSQL C++ library
#include <string>
#include <regex>

int get_query_plan_cardinality(const std::string& query, const std::string& conninfo) {
    try {
        pqxx::connection conn(conninfo);
        if (!conn.is_open()) {
            std::cerr << "Failed to connect to the database." << std::endl;
            return -1;
        } else {
            std::cout << "Connected to the database." << std::endl;
        }

        pqxx::work txn(conn);

        std::string explain_query = "EXPLAIN " + query;
        pqxx::result result = txn.exec(explain_query);

        int cardinality = -1;
        std::regex rows_regex(R"(rows=(\d+))");
        std::smatch match;

        for (const auto& row : result) {
            std::string line = row[0].as<std::string>();
            if (std::regex_search(line, match, rows_regex)) {
                cardinality = std::stoi(match[1].str());
                break;
            }
        }

        return cardinality;
    } catch (const std::exception& e) {
        std::cerr << "Error while connecting to PostgreSQL or getting query plan: " << e.what() << std::endl;
        return -1;
    }
}
