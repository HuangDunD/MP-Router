#ifndef BMSQL_META_H
#define BMSQL_META_H

#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <fstream>
#include <sstream>
#include <iostream>
#include <stdexcept> // For exceptions
#include <utility>   // For std::pair

#ifndef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

using rapidjson::Document;
using rapidjson::Value;
using rapidjson::kArrayType;
using rapidjson::kObjectType;
using rapidjson::kNumberType;
using rapidjson::kStringType;
using rapidjson::kFalseType;
using rapidjson::kTrueType;

namespace BmSql {
    // Forward declaration
    struct TableInfo;

    // Structure to hold column metadata
    struct ColumnInfo {
        int id = -1;
        std::string name;
        bool is_affinity = false;
        int region_size = 1000;
        const TableInfo *table = nullptr; // Back pointer

        ColumnInfo() = default;

        ColumnInfo(int p_id, std::string p_name, bool p_is_affinity, int p_region_size)
            : id(p_id), name(std::move(p_name)), is_affinity(p_is_affinity), region_size(p_region_size),
              table(nullptr) {
        }
    };

    // Structure to hold table metadata including its columns
    struct TableInfo {
        int id = -1;
        std::string name;
        std::vector<ColumnInfo> columns;

        // Internal maps for faster column lookup *within this table*
        std::unordered_map<int, size_t> columnIdToIndexMap;
        std::unordered_map<std::string, size_t> columnNameToIndexMap;

        TableInfo() = default;

        TableInfo(int p_id, std::string p_name) : id(p_id), name(std::move(p_name)) {
        }

        size_t getColumnIndexById(int colId) const {
            auto it = columnIdToIndexMap.find(colId);
            if (it != columnIdToIndexMap.end()) { return it->second; }
            throw std::out_of_range("Column ID " + std::to_string(colId) + " not found in table '" + name + "'");
        }

        size_t getColumnIndexByName(const std::string &colName) const {
            auto it = columnNameToIndexMap.find(colName);
            if (it != columnNameToIndexMap.end()) { return it->second; }
            throw std::out_of_range("Column Name '" + colName + "' not found in table '" + name + "'");
        }

        const ColumnInfo *getColumnById(int colId) const {
            auto it = columnIdToIndexMap.find(colId);
            if (it != columnIdToIndexMap.end() && it->second < columns.size()) { return &columns[it->second]; }
            return nullptr;
        }

        const ColumnInfo *getColumnByName(const std::string &colName) const {
            auto it = columnNameToIndexMap.find(colName);
            if (it != columnNameToIndexMap.end() && it->second < columns.size()) { return &columns[it->second]; }
            return nullptr;
        }
    };

    // Class to manage the BMSQL schema loaded from JSON
    class Meta {
    public:
        Meta() = default;

        Meta(const Meta &) = delete;

        Meta &operator=(const Meta &) = delete;

        Meta(Meta &&) = default;

        Meta &operator=(Meta &&) = default;

        int getRegionSizeByColumnId(int columnId) const {
            return idToRegionSize.find(columnId)->second;
        }

        /**
         * @brief Checks if a specific column within a specific table is an affinity column.
         * Uses the global column ID for lookup and verifies against the provided table ID.
         * @param tableId The ID of the table the column should belong to.
         * @param columnId The global ID of the column to check.
         * @return True if the column with 'columnId' exists, belongs to the table with 'tableId',
         * and its 'is_affinity' flag is true. Returns false otherwise (e.g., IDs not found,
         * column belongs to a different table, or is_affinity is false).
         */
        bool isColumnAffinity(int tableId, int columnId) const {
            return columunIsAffinity.find(columnId)->second;
        }

        bool loadFromJsonFile(const std::string &filename) {
            // 1. 读取文件到 std::string
            std::ifstream ifs(filename, std::ios::in | std::ios::binary);
            if (!ifs) {
                std::cerr << "Error [BmSql::Meta]: Cannot open JSON meta file: " << filename << '\n';
                return false;
            }
            std::ostringstream oss;
            oss << ifs.rdbuf();
            std::string jsonStr = oss.str();

            // 2. 解析
            rapidjson::Document doc;
            doc.Parse(jsonStr.c_str());
            if (doc.HasParseError()) {
                const char *msg =
#ifdef RAPIDJSON_ERROR_EN_H_            // 有 en.h
            rapidjson::GetParseError_En(doc.GetParseError());
#else                                   // Fallback （较老版本）
                        rapidjson::GetParseErrorFunc()(doc.GetParseError());
#endif
                std::cerr << "Error [BmSql::Meta]: RapidJSON parsing failed for file: " << filename << '\n'
                        << "Message : " << msg << '\n'
                        << "Offset  : " << doc.GetErrorOffset() << '\n';
                return false;
            }
            if (!doc.IsArray()) {
                std::cerr << "Error [BmSql::Meta]: JSON root is not an array in file: " << filename << '\n';
                return false;
            }

            // 3. 清空旧数据
            tables_.clear();
            tableIdToIndexMap_.clear();
            tableNameToIndexMap_.clear();
            globalColumnIdLocationMap_.clear();
            globalColumnNameLocationMap_.clear();
            idToNameMap_.clear();
            idToRegionSize.clear();
            columunIsAffinity.clear();

            tables_.reserve(doc.Size());

            // 4. 遍历表
            for (auto itTable = doc.Begin(); itTable != doc.End(); ++itTable) {
                const rapidjson::Value &t = *itTable;
                if (!t.IsObject() || !t.HasMember("id") || !t["id"].IsInt() ||
                    !t.HasMember("name") || !t["name"].IsString() ||
                    !t.HasMember("columns") || !t["columns"].IsArray()) {
                    std::cerr << "Error [BmSql::Meta]: Invalid table object structure.\n";
                    return false;
                }

                TableInfo tbl{t["id"].GetInt(), t["name"].GetString()};
                const rapidjson::Value &cols = t["columns"];
                tbl.columns.reserve(cols.Size());

                // 4.1 遍历列
                for (auto itCol = cols.Begin(); itCol != cols.End(); ++itCol) {
                    const rapidjson::Value &c = *itCol;
                    if (!c.IsObject() || !c.HasMember("id") || !c["id"].IsInt() ||
                        !c.HasMember("name") || !c["name"].IsString() ||
                        !c.HasMember("is_affinity") || !c["is_affinity"].IsBool() ||
                        !c.HasMember("REGION_SIZE") || !c["REGION_SIZE"].IsInt()) {
                        std::cerr << "Error [BmSql::Meta]: Invalid column object structure.\n";
                        return false;
                    }
                    tbl.columns.emplace_back(
                        c["id"].GetInt(),
                        c["name"].GetString(),
                        c["is_affinity"].GetBool(),
                        c["REGION_SIZE"].GetInt()
                    );
                }

                tables_.emplace_back(std::move(tbl));
            }

            // 5. 构建辅助索引
            buildIndexMaps_();

            std::cout << "Info [BmSql::Meta]: Successfully loaded metadata via RapidJSON from: "
                    << filename << '\n';
            return true;
        }

        // --- Public Accessor Interfaces (const methods) ---

        const TableInfo *getTableById(int tableId) const {
            auto it = tableIdToIndexMap_.find(tableId);
            if (it != tableIdToIndexMap_.end() && it->second < tables_.size()) {
                return &tables_[it->second];
            }
            return nullptr;
        }

        const TableInfo *getTableByName(const std::string &tableName) const {
            auto it = tableNameToIndexMap_.find(tableName);
            if (it != tableNameToIndexMap_.end() && it->second < tables_.size()) {
                return &tables_[it->second];
            }
            return nullptr;
        }

        const std::vector<TableInfo> &getAllTables() const {
            return tables_;
        }

        const ColumnInfo *getColumnByGlobalId(int globalColumnId) const {
            auto it = globalColumnIdLocationMap_.find(globalColumnId);
            if (it != globalColumnIdLocationMap_.end()) {
                size_t tableIdx = it->second.first;
                size_t colIdx = it->second.second;
                if (tableIdx < tables_.size() && colIdx < tables_[tableIdx].columns.size()) {
                    return &tables_[tableIdx].columns[colIdx];
                }
            }
            return nullptr;
        }

        const ColumnInfo *getColumnByGlobalName(const std::string &globalColumnName) const {
            auto it = globalColumnNameLocationMap_.find(globalColumnName);
            if (it != globalColumnNameLocationMap_.end()) {
                size_t tableIdx = it->second.first;
                size_t colIdx = it->second.second;
                if (tableIdx < tables_.size() && colIdx < tables_[tableIdx].columns.size()) {
                    return &tables_[tableIdx].columns[colIdx];
                }
            }
            return nullptr;
        }

        const ColumnInfo *getColumnByNameInTable(const std::string &colName, int tableId) const {
            const TableInfo *table = getTableById(tableId);
            return table ? table->getColumnByName(colName) : nullptr;
        }

        const ColumnInfo *getColumnByNameInTable(const std::string &colName, const std::string &tableName) const {
            const TableInfo *table = getTableByName(tableName);
            return table ? table->getColumnByName(colName) : nullptr;
        }

        const ColumnInfo *getColumnByIdInTable(int colId, int tableId) const {
            const TableInfo *table = getTableById(tableId);
            return table ? table->getColumnById(colId) : nullptr;
        }

        const ColumnInfo *getColumnByIdInTable(int colId, const std::string &tableName) const {
            const TableInfo *table = getTableByName(tableName);
            return table ? table->getColumnById(colId) : nullptr;
        }

        // --- NEW INTERFACE: Get Name by Global ID ---
        // Returns the name corresponding to any table or column ID.
        // Returns an empty string if the ID is not found.
        std::string getNameById(int id) const {
            auto it = idToNameMap_.find(id);
            if (it != idToNameMap_.end()) {
                return it->second; // Return the found name
            }
            // Optional: Log a warning if ID not found
            // std::cerr << "Warning [BmSql::Meta]: ID " << id << " not found in idToNameMap." << std::endl;
            return ""; // Return empty string as indicator
        }

        const ColumnInfo *getColumnInfo(const std::string &tableName, const std::string &columnName) const {
            const TableInfo *table = getTableByName(tableName);
            return table ? table->getColumnByName(columnName) : nullptr;
        }

    public:
        // --- Private Data Members ---
        std::vector<TableInfo> tables_; // Main storage

        std::map<int, int> idToRegionSize;
        std::map<int, bool> columunIsAffinity;

        // Helper maps for fast global lookups
        std::unordered_map<int, size_t> tableIdToIndexMap_;
        std::unordered_map<std::string, size_t> tableNameToIndexMap_;
        std::unordered_map<int, std::pair<size_t, size_t> > globalColumnIdLocationMap_;
        std::unordered_map<std::string, std::pair<size_t, size_t> > globalColumnNameLocationMap_;

        std::map<int, std::string> idToNameMap_;


        void buildIndexMaps_() {
            // Clear all maps before rebuilding
            tableIdToIndexMap_.clear();
            tableNameToIndexMap_.clear();
            globalColumnIdLocationMap_.clear();
            globalColumnNameLocationMap_.clear();
            idToNameMap_.clear();
            idToRegionSize.clear(); // Clear previously added map
            columunIsAffinity.clear(); // Clear NEW map

            // Optional: Reserve space for maps for potential minor performance gain
            tableIdToIndexMap_.reserve(tables_.size());
            tableNameToIndexMap_.reserve(tables_.size());
            // Calculate total number of columns for reserving other maps (more involved)
            size_t total_columns = 0;
            for (const auto &table: tables_) { total_columns += table.columns.size(); }
            globalColumnIdLocationMap_.reserve(total_columns);
            globalColumnNameLocationMap_.reserve(total_columns);
            // For std::map, reserve doesn't exist. For std::unordered_map, it helps avoid rehashes.

            // Iterate through tables and columns to populate all maps
            for (size_t tableIdx = 0; tableIdx < tables_.size(); ++tableIdx) {
                TableInfo &table = tables_[tableIdx]; // Need non-const ref to modify table's internal maps

                // Populate maps related to the table itself
                tableIdToIndexMap_[table.id] = tableIdx;
                tableNameToIndexMap_[table.name] = tableIdx;
                idToNameMap_[table.id] = table.name;
                // Tables don't have region size or affinity, only columns do

                // Clear and populate per-table column maps
                table.columnIdToIndexMap.clear();
                table.columnNameToIndexMap.clear();
                table.columnIdToIndexMap.reserve(table.columns.size());
                table.columnNameToIndexMap.reserve(table.columns.size());

                // Iterate through columns within the current table
                for (size_t colIdx = 0; colIdx < table.columns.size(); ++colIdx) {
                    ColumnInfo &col = table.columns[colIdx]; // Need non-const ref to set back-pointer
                    col.table = &table; // Set back-pointer

                    // Populate global column maps
                    globalColumnIdLocationMap_[col.id] = {tableIdx, colIdx};
                    globalColumnNameLocationMap_[col.name] = {tableIdx, colIdx};

                    // Populate the direct lookup maps
                    idToNameMap_[col.id] = col.name;
                    idToRegionSize[col.id] = col.region_size; // Populate region size map
                    columunIsAffinity[col.id] = col.is_affinity; // Populate affinity map

                    // Populate per-table column maps
                    table.columnIdToIndexMap[col.id] = colIdx;
                    table.columnNameToIndexMap[col.name] = colIdx;
                } // End column loop
            } // End table loop
        } // End buildIndexMaps_
    }; // End class BmSql::Meta
} // End namespace BmSql

#endif // BMSQL_META_H
