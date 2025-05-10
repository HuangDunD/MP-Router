#ifndef REGION_PROCESSOR_H
#define REGION_PROCESSOR_H

#include <vector>
#include <string>
#include <cstdint>
#include <stdexcept> // For std::runtime_error

// --- MODIFICATION START ---
#include "Logger.h" // Include Logger definition
#include "parse.h"     // Include SQLInfo definition
#include "bmsql_meta.h"   // Include definition for TPCHMeta and extern TPCH_META

class TPCHMeta; // REMOVED Forward declaration


/**
 * @brief Processes raw data to generate region IDs based on workload type.
 * Encapsulates the logic previously in generate_and_process_region_ids.
 */
class RegionProcessor {
public:
    /**
     * @brief Constructor.
     * @param logger A reference to the logger object for logging messages.
     * @throws std::runtime_error if required global configurations (like REGION_SIZE) are invalid.
     */
    explicit RegionProcessor(Logger &logger);

    // Disable copy constructor and copy assignment
    RegionProcessor(const RegionProcessor &) = delete;

    RegionProcessor &operator=(const RegionProcessor &) = delete;

    /**
     * @brief Parses data based on WORKLOAD_MODE and generates region IDs.
     * @param data Raw data received from the client.
     * @param out_region_ids Output vector to store generated combined region IDs.
     * @param raw_txn
     * @return bool Returns false if a critical error occurred that prevents further processing
     * (e.g., invalid REGION_SIZE during processing), true otherwise.
     * Non-critical errors (like parsing errors or invalid keys) might be logged
     * but the function may still return true.
     */
    bool generateRegionIDs(const std::string &data, std::vector<uint64_t> &out_region_ids,
                           const BmSql::Meta &bmsqlMeta, std::string &raw_txn);

private:
    Logger &logger_; // Reference to the logger instance

    // --- Private Helper Methods ---

    /**
     * @brief Processes data assuming YCSB workload format.
     * @param data The raw input data string.
     * @param out_region_ids Vector to populate with generated region IDs.
     * @return bool False on critical errors (invalid REGION_SIZE), true otherwise.
     */
    bool processYCSB(const std::string &data, std::vector<uint64_t> &out_region_ids);

    /**
     * @brief Processes data assuming TPC-H workload format (SQL).
     * @param data The raw input data string (SQL query).
     * @param out_region_ids Vector to populate with generated region IDs.
     * @param raw_txn
     * @return bool False on critical errors (invalid REGION_SIZE), true otherwise.
     */
    bool processTPCH(const std::string &data, const BmSql::Meta &bmsqlMeta, std::vector<uint64_t> &out_region_ids, std::string &raw_txn);

    // --- Dependencies (Accessed via extern as in original code) ---
    // These are kept extern for minimal changes from original structure,
    // but ideally would be passed via constructor or method parameters.
    friend class ApplicationDependencyAccessor; // Hypothetical friend class for accessing globals if needed
};

extern TPCHMeta *TPCH_META; // Pointer to TPC-H metadata

#endif // REGION_PROCESSOR_H
