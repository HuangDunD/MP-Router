#include "region_generator.h"
#include "../log/Logger.h" // Include Logger definition

// --- MODIFICATION START ---
#include "../db_meta.h"   // Include definition for TPCHMeta and extern TPCH_META
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

bool RegionProcessor::generateRegionIDs(const std::string &data, std::vector<uint64_t> &out_region_ids) {
    out_region_ids.clear(); // Ensure the output vector is empty

    // Check REGION_SIZE again in case it was modified externally (though unlikely if const)
    if (REGION_SIZE <= 0) {
        logger_.log("ERROR: REGION_SIZE (", REGION_SIZE, ") is not positive during generateRegionIDs call.");
        return false; // Critical error
    }

#if WORKLOAD_MODE == 0
    // YCSB workload
    return processYCSB(data, out_region_ids);
#elif WORKLOAD_MODE == 1
    // TPCH workload
    return processTPCH(data, out_region_ids);
#else
    logger_.log("ERROR: Unknown WORKLOAD_MODE defined: ", WORKLOAD_MODE);
    return true; // Or false, depending on how you want to handle unknown modes
#endif
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
        logger_.log(ss_ids.str());
        // NOTE: Graph building (metis.build_internal_graph) is NOT called here anymore.
        // It should be called in the calling function (e.g., process_client_data) if needed.
    } else {
        logger_.log("No valid YCSB region IDs were generated (keys might have been negative).");
    }
    return true; // Success (or non-critical failure)
}

// --- Private TPCH Implementation ---
bool RegionProcessor::processTPCH(const std::string &data, std::vector<uint64_t> &out_region_ids) {
    logger_.log("Processing TPCH data (SQL)...");
    try {
        SQLInfo sql_info = parseTPCHSQL(data); // Assume parseTPCHSQL is available

        if (sql_info.type == SQLType::SELECT || sql_info.type == SQLType::UPDATE) {
            std::string type_str = (sql_info.type == SQLType::SELECT) ? "SELECT" : "UPDATE";
            logger_.log("Parsed ", type_str, " statement.");

            if (sql_info.tableNames.empty() || sql_info.columnNames.empty()) {
                logger_.log("Warning: Missing table or column name in ", type_str, ". Skipping region ID generation.");
                return true; // Non-critical issue
            }
            if (sql_info.keyVector.empty()) {
                logger_.log("No keys found in ", type_str, " WHERE clause. Skipping region ID generation.");
                return true; // Non-critical issue
            }

            std::string table_name = sql_info.tableNames[0];
            std::string column_name = sql_info.columnNames[0];

            unsigned int table_id;
            unsigned int column_id;
            // Safely get table and column IDs
            try {
                // Check if TPCH_META is valid
                if (!TPCH_META) {
                    throw std::runtime_error("TPCH_META is null.");
                }
                table_id = TPCHMeta::tablenameToID.at(table_name); // Use .at() for bounds checking
                // Ensure table_id is valid index before accessing nested maps/vectors
                if (table_id >= TPCHMeta::columnNameToID.size() || table_id >= TPCH_META->partition_column_ids.size()) {
                    throw std::out_of_range(
                        "Table ID (" + std::to_string(table_id) + ") out of range for metadata access.");
                }
                column_id = TPCHMeta::columnNameToID[table_id].at(column_name); // Use .at()
            } catch (const std::out_of_range &oor) {
                logger_.log("ERROR: Invalid table/column name ('", table_name, "'/'", column_name,
                            "') or ID lookup failed. ", oor.what());
                return true; // Invalid name/ID, log but allow sending response
            } catch (const std::exception &e) {
                logger_.log("ERROR looking up TPCH metadata: ", e.what());
                return true; // Metadata error, log but allow sending response
            }

            // Check if the queried column is the partition key for that table
            if (TPCH_META->partition_column_ids[table_id] != column_id) {
                logger_.log("Query key (", table_name, ".", column_name,
                            ") is not the partition key for table ", table_id,
                            ". Skipping region ID generation.");
                return true; // Not an error, just skipping
            }

            // Partition key matches, proceed to calculate combined IDs
            logger_.log("Query key matches partition key (", table_name, ".", column_name,
                        "). Processing keys...");

            for (const auto &key: sql_info.keyVector) {
                if (key < 0) {
                    logger_.log("Warning: Skipping negative key ", key, " for table ", table_name);
                    continue; // Skip negative keys
                }
                // REGION_SIZE check already done in generateRegionIDs

                // Calculate inner region ID (ensure unsigned)
                auto _inner_region_id = static_cast<unsigned int>(key / REGION_SIZE);

                // Create Region object using the looked-up table_id
                Region current_region(table_id, _inner_region_id);

                // Serialize to uint64_t
                uint64_t combined_id = current_region.serializeToUint64();
                out_region_ids.push_back(combined_id);
            } // End key loop

            if (!out_region_ids.empty()) {
                // Log the combined IDs being sent
                std::stringstream ss_ids;
                ss_ids << "Generated TPCH Combined Region IDs (" << out_region_ids.size() << "): ";
                for (size_t i = 0; i < std::min(out_region_ids.size(), (size_t) 5); ++i)
                    ss_ids << out_region_ids[i] << " "; // Log first few
                if (out_region_ids.size() > 5) ss_ids << "...";
                logger_.log(ss_ids.str());
                // NOTE: Graph building (metis.build_internal_graph) is NOT called here anymore.
            } else {
                logger_.log("No valid TPCH region IDs generated (keys might have been negative or filtered).");
            }
        } else if (sql_info.type == SQLType::JOIN) {
            logger_.log("Received JOIN statement. Region ID generation not implemented for JOINs.");
            // Existing JOIN logic (e.g., cardinality update) can remain here if needed
        } else {
            std::string truncated_data = data.substr(0, 150) + (data.length() > 150 ? "..." : "");
            logger_.log("Unknown or unhandled SQL type received. SQL: ", truncated_data);
        }
    } catch (const std::exception &e) {
        // Catch errors during parsing or metadata lookup
        logger_.log("ERROR processing SQL: ", e.what());
        // Depending on severity, you might want to stop or just log and continue
        return true; // Allow sending response by default on SQL processing errors
    }

    return true; // Success (or non-critical failure)
}

