#pragma once

#include "seron_tv_estimator.h"
#include <pqxx/pqxx>
#include <iostream>
#include <string>

namespace seron_tv {
struct Sample {
    std::vector<std::vector<PhysicalValue>> columns;
    bool complete = false;
};

// Each attempt samples whole pages without a row LIMIT. LIMIT here is used
// only to distinguish genuinely small relations before a full scan is accepted.
inline Sample sample_relation(pqxx::transaction_base& db, const std::string& relation,
                              const std::vector<std::string>& columns,
                              const std::vector<uint64_t>& domains, std::ostream& log) {
    if (columns.empty() || columns.size() != domains.size())
        throw std::invalid_argument("Invalid sampling column specification");
    std::string select = "SELECT tableoid::bigint, "
        "split_part(trim(leading '(' from ctid::text), ',', 1)::bigint, "
        "split_part(trim(trailing ')' from ctid::text), ',', 2)::int";
    for (const auto& column : columns) select += ", " + db.quote_name(column);
    // relation is a server-returned regclass identifier, not user SQL.
    select += " FROM " + relation;
    auto rows = db.exec(select + " LIMIT " + std::to_string(FullScanRowLimit + 1));
    bool complete = rows.size() <= FullScanRowLimit;
    for (int attempt = -1; attempt < 4; ++attempt) {
        static const int rates[] = {1, 5, 20, 100};
        if (attempt == -1 && !complete) continue;
        if (attempt >= 0) {
            complete = rates[attempt] == 100;
            rows = db.exec(select + (complete ? "" : " TABLESAMPLE SYSTEM (" +
                std::to_string(rates[attempt]) + ") REPEATABLE (33)"));
        }
        Sample sample;
        sample.complete = complete;
        sample.columns.resize(columns.size());
        for (const auto& row : rows) {
            const uint64_t page = (row[0].as<uint64_t>() << 32) | row[1].as<uint64_t>();
            const uint32_t offset = row[2].as<uint32_t>();
            for (size_t j=0; j<columns.size(); ++j) {
                const auto field = row[static_cast<pqxx::row::size_type>(j+3)];
                if (!field.is_null()) sample.columns[j].push_back({page, field.as<uint64_t>(), offset});
            }
        }
        bool sufficient = true;
        for (size_t j=0; j<columns.size(); ++j) {
            auto& values = sample.columns[j];
            std::sort(values.begin(), values.end(), [](const auto& a, const auto& b) {
                return a.page != b.page ? a.page < b.page : a.offset < b.offset;
            });
            const auto stats = statistics(values, domains[j], complete);
            sufficient &= stats.sufficient;
            log << "Seron physical sample " << relation << '.' << columns[j]
                << " source=" << (complete ? "full-scan" : "system-sample")
                << " percent=" << (complete ? 100 : rates[attempt])
                << " rows=" << stats.rows << " pages=" << stats.pages
                << " adjacent_pairs=" << stats.adjacent_pairs
                << " cross_page_pairs=" << stats.cross_page_pairs
                << " sufficient=" << stats.sufficient << '\n';
        }
        if (sufficient || complete) return sample;
    }
    throw std::logic_error("Unreachable sampling state");
}
} // namespace seron_tv
