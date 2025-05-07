#include "region_generator.h"
#include "../log/Logger.h" // Include Logger definition

// --- MODIFICATION START ---
#include "../config.h"    // Include definition for WORKLOAD_MODE and REGION_SIZE
// --- MODIFICATION END ---


// Include necessary implementation headers
#include <vector>
#include <string>
#include <cstdint>
#include <regex>
#include <stdexcept>
#include <sstream>
#include <algorithm> // For std::sort, std::unique, std::min
#include <thread>    // For std::hash<std::thread::id> - Used only for logging context

// Assume these are fully defined/included elsewhere
#include "../region/region.h"     // Definition for Region struct/class


RegionProcessor::RegionProcessor(Logger &logger) : logger_(logger) {
    // Constructor can perform initial checks if needed
    if (REGION_SIZE <= 0) {
        // Log this critical configuration error immediately
        logger_.log("CRITICAL ERROR: RegionProcessor initialized when REGION_SIZE (", REGION_SIZE,
                    ") is not positive.");
        // Throwing here prevents the object from being used in an invalid state
        throw std::runtime_error("RegionProcessor Error: REGION_SIZE must be positive.");
    }
    logger_.log("RegionProcessor initialized.");
}

bool RegionProcessor::generateRegionIDs(const std::string &data, std::vector<uint64_t> &out_region_ids,
                                        const BmSql::Meta &bmsqlMeta, std::string &raw_txn) {
    out_region_ids.clear(); // Ensure the output vector is empty

    // Check REGION_SIZE again in case it was modified externally (though unlikely if const)
    if (REGION_SIZE <= 0) {
        logger_.log("ERROR: REGION_SIZE (", REGION_SIZE, ") is not positive during generateRegionIDs call.");
        return false; // Critical error
    }

    return processTPCH(data, bmsqlMeta, out_region_ids, raw_txn);

}

// --- Private YCSB Implementation ---
bool RegionProcessor::processYCSB(const std::string &data, std::vector<uint64_t> &out_region_ids) {
    logger_.log("Processing YCSB data...");
    std::vector<int> ycsb_keys;
    try {
        std::regex key_pattern(R"(YCSB_KEY\s*=\s*(\d+))"); // Matches YCSB_KEY = <number>
        std::smatch matches;
        std::string::const_iterator search_start(data.cbegin());
        while (std::regex_search(search_start, data.cend(), matches, key_pattern)) {
            ycsb_keys.push_back(std::stoi(matches[1].str()));
            search_start = matches.suffix().first;
        }
        // Optional: Remove duplicates if needed
        std::sort(ycsb_keys.begin(), ycsb_keys.end());
        ycsb_keys.erase(std::unique(ycsb_keys.begin(), ycsb_keys.end()), ycsb_keys.end());
    } catch (const std::exception &e) {
        logger_.log("ERROR parsing YCSB keys: ", e.what());
        // Parsing error is often not fatal, log and continue (return true)
        return true;
    }

    if (ycsb_keys.empty()) {
        logger_.log("No YCSB keys found in data.");
        return true; // Not an error, just no keys to process
    }

    const unsigned int ycsb_table_id = 0; // Example: table 0 for YCSB

    for (const auto &key: ycsb_keys) {
        if (key < 0) {
            logger_.log("Warning: Skipping negative YCSB key ", key);
            continue;
        }
        // REGION_SIZE check already done in generateRegionIDs

        // Calculate inner region ID (ensure unsigned)
        unsigned int _inner_region_id = static_cast<unsigned int>(key / REGION_SIZE);

        // Create Region object
        Region current_region(ycsb_table_id, _inner_region_id);

        // Serialize to uint64_t
        uint64_t combined_id = current_region.serializeToUint64();
        out_region_ids.push_back(combined_id);
    }

    if (!out_region_ids.empty()) {
        // Log the combined IDs being sent
        std::stringstream ss_ids;
        ss_ids << "Generated YCSB Combined Region IDs (" << out_region_ids.size() << "): ";
        for (size_t i = 0; i < std::min(out_region_ids.size(), (size_t) 5); ++i) ss_ids << out_region_ids[i] << " ";
        // Log first few
        if (out_region_ids.size() > 5) ss_ids << "...";
        logger_.log(ss_ids.str().c_str());
        // NOTE: Graph building (metis.build_internal_graph) is NOT called here anymore.
        // It should be called in the calling function (e.g., process_client_data) if needed.
    } else {
        logger_.log("No valid YCSB region IDs were generated (keys might have been negative).");
    }
    return true; // Success (or non-critical failure)
}

std::unordered_map<int, long long> col_cardinality;
std::mutex col_mutex;
std::atomic<long long> times{0};

bool RegionProcessor::processTPCH(const std::string &data, const BmSql::Meta &bmsqlMeta,
                                  std::vector<uint64_t> &out_region_ids, std::string &raw_txn) {
    try {
        // 1. Call the BMSQL parser
        std::vector<SQLInfo> sql_infos = parseTPCHSQL(data, bmsqlMeta.idToNameMap_, raw_txn);

        logger_.log("Parsed ", sql_infos.size(), " SQL info block(s).");

        // 2. Loop through each parsed block
        for (size_t i = 0; i < sql_infos.size(); ++i) {
            const auto &current_sql_info = sql_infos[i];
            // logger_.log("Processing block ", i + 1, " of ", sql_infos.size(), "...");

            // 3. Process based on type
            if (current_sql_info.type == SQLType::SELECT || current_sql_info.type == SQLType::UPDATE) {
                std::string type_str = (current_sql_info.type == SQLType::SELECT) ? "SELECT" : "UPDATE";
                // logger_.log("Block ", i + 1, ": Parsed ", type_str, " statement.");


                // Store the column name parsed (if any) for potential reference/warning later
                size_t column_count = current_sql_info.columnNames.size();
                int affinityColumn = -1;
                int ser_num = -1;

                for (int j = 0; j < column_count; j++) {
                    col_cardinality[current_sql_info.columnIDs[j]]++;
                    if (bmsqlMeta.isColumnAffinity(current_sql_info.tableIDs[0], current_sql_info.columnIDs[j])) {
                        affinityColumn = current_sql_info.columnIDs[j];
                        ser_num = j;
                        break;
                    }
                }

                if (affinityColumn == -1) {
                    logger_.log("Block ", i + 1, ": Warning: No affinity column found in table '",
                                current_sql_info.tableNames[0],
                                "'.");
                } else {
                    auto inner_key = current_sql_info.keyVector[ser_num] / bmsqlMeta.getRegionSizeByColumnId(
                                         current_sql_info.columnIDs[ser_num]);

                    Region current_region(current_sql_info.tableIDs[0], inner_key);
                    uint64_t combined_id = current_region.serializeToUint64();
                    out_region_ids.push_back(combined_id);
                    // logger_.log("Block ", i + 1, ": Found affinity column ID ", affinityColumn, " in table '",
                                // current_sql_info.tableNames[0], "'.");
                }
                // --- End of logic for SELECT/UPDATE block ---
            } else if (current_sql_info.type == SQLType::JOIN) {
                for (auto &columenID: current_sql_info.columnIDs) {
                    col_cardinality[columenID]++;
                }
                // ... (JOIN handling remains the same) ...
                std::stringstream ss_join_tables;
                // ss_join_tables << "Block " << (i + 1) << ": Received JOIN block involving tables: ";
                for (const auto &name: current_sql_info.tableNames) ss_join_tables << name << " ";
                ss_join_tables << ". Region ID generation not applicable.";
                logger_.log(ss_join_tables.str().c_str());
            } else {
                // ... (UNKNOWN handling remains the same) ...
                logger_.log("Block ", i + 1, ": Unknown or unhandled SQL type encountered. Skipping block.");
            }
        } // End loop through sql_infos
    } catch (const std::exception &e) {
        logger_.log("ERROR processing BMSQL SQL data: ", e.what());
        return true;
    } catch (...) {
        logger_.log("ERROR processing BMSQL SQL data: Unknown exception occurred.");
        return true;
    }
    times.fetch_add(1);
    if (times % 2000 == 0) {
        std::ofstream ofs("col_cardinality.csv", std::ios::trunc);
        assert(ofs.is_open());
        for (auto &[id,cnt]: col_cardinality)
            ofs << id  << ',' << cnt << '\n';
        ofs.close();
        std::cout << "finish"<<std::endl;
    }

    return true;
}
