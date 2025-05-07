#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <utility>

#include "queryplan_cardinality.h"
#include "db_meta.h"
#include "util/json_config.h"
#include "bmsql_meta.h"
#include "parse.h"

// ---------- 1. 拆分工具 ----------
std::vector<std::string> split_sql_statements(const std::string &all_sql)
{
    std::vector<std::string> stmts;
    std::ostringstream current;
    std::istringstream in(all_sql);
    std::string line;

    auto trim = [](const std::string &s) -> std::string {
        auto b = s.find_first_not_of(" \t\r\n");
        auto e = s.find_last_not_of(" \t\r\n");
        return (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
    };

    while (std::getline(in, line)) {
        std::string t = trim(line);
        if (t.empty()) continue;
        current << line << '\n';
        if (t.back() == ';') {                  // 完整一条
            stmts.push_back(current.str());
            current.str(""); current.clear();
        }
    }
    auto leftover = trim(current.str());
    if (!leftover.empty()) stmts.push_back(leftover);
    return stmts;
}

static void process_single_sql(const std::string            &sql,
                               const BmSql::Meta            &meta,
                               std::unordered_map<int,long long> &col_cardinality)
{
    std::string raw_txn;
    auto sql_infos = parseTPCHSQL(sql, meta.idToNameMap_, raw_txn);
    for (const auto &info : sql_infos) {
        if (info.type == SQLType::SELECT ||
            info.type == SQLType::UPDATE ||
            info.type == SQLType::JOIN)               // 目前三种操作都会累加
        {
            for (int col : info.columnIDs) ++col_cardinality[col];
        }
    }
}

// ---------- 2. 单字符串版本接口 ----------
void analyze_offline_workload_from_string(
                                          const std::string& config_path,
                                          const std::string& meta_path)
{
    /* (1) 配置、元数据 */
    auto json_cfg = JsonConfig::load_file(config_path);
    REGION_SIZE   = json_cfg.get("ycsb").get("key_cnt_per_partition").get_int64();

    BmSql::Meta meta;
    meta.loadFromJsonFile(meta_path);

    std::ifstream ifs("col_cardinality.csv");
    std::unordered_map<int,long long> col_cardinality;
    std::string line;
    size_t line_no = 0;
    while (std::getline(ifs, line)) {
        ++line_no;
        if (line.empty()) continue;
        auto pos = line.find(',');
        if (pos == std::string::npos) {
            std::cerr << "Error: line " << line_no << " is not valid: " << line << std::endl;
            continue;
        }
        try {
            int        col = std::stoi(line.substr(0, pos));
            long long  cnt = std::stoll(line.substr(pos + 1));
            col_cardinality[col] = cnt;                // 注意：直接覆盖旧值
        } catch (const std::exception& e) {
            std::cerr << e.what()<<std::endl;
        }
    }
    /* ---------- (1) 先找每张表的最大列 ---------- */
    std::map<int /*tbl*/, std::pair<int /*col*/, long long /*card*/>> best_col;
    for (auto &[col_id, card] : col_cardinality) {
        int tbl_id = meta.getTableIdByColumnId(col_id);
        auto &best = best_col[tbl_id];
        if (best.second < card) best = {col_id, card};
    }

    /* ---------- (2) 收集所有记录并排序（可选） ---------- */
    struct Rec { int tbl; int col; long long card; };
    std::vector<Rec> rows;
    rows.reserve(col_cardinality.size());

    for (auto &[col_id, card] : col_cardinality) {
        rows.push_back({ meta.getTableIdByColumnId(col_id), col_id, card });
    }

    std::sort(rows.begin(), rows.end(),
              [](const Rec &a, const Rec &b){
                  return a.tbl == b.tbl ? a.card > b.card : a.tbl < b.tbl;
              });

    /* ---------- (3) 打印表头 ---------- */
    std::cout << "\n"
              << std::left
              << std::setw(8)  << "Tbl‑ID"   << ' '
              << std::setw(16) << "Table‑Name" << ' '
              << std::setw(8)  << "Col‑ID"   << ' '
              << std::setw(20) << "Column‑Name" << ' '
              << std::setw(12) << "Cardinality"  << '\n'
              << "--------------------------------------------------------------------------\n";

    /* ---------- (4) 打印每一行 ---------- */
    for (const auto &r : rows) {
        bool is_best = (best_col[r.tbl].first == r.col);

        std::cout << std::left
                  << std::setw(8)  << r.tbl                         << ' '
                  << std::setw(16) << meta.getNameById(r.tbl)  << ' '
                  << std::setw(8)  << r.col                         << ' '
                  << std::setw(20) << meta.getNameById(r.col) << ' '
                  << std::right
                  << std::setw(12) << r.card
                  << (is_best ? "  <-- MAX" : "")
                  << '\n';
    }
}

/* ----------- 示例 main ----------- */
int main() {
    std::string workload_sql =
        "SELECT col1, col2 FROM customer WHERE id = 1;\n"
        "UPDATE order_line SET amount = 10 WHERE ol_id = 5;\n"
        "SELECT col3 FROM supplier;\n";   // 多条 SQL 用 ; 结尾

    analyze_offline_workload_from_string(
                                         "../../config/ycsb_config.json",
                                         "../../config/tpcc_meta.json");
    return 0;
}
