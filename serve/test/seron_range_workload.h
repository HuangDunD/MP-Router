#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

// SQL-template adapters only: no cross-table identity or preferred routing key.
// Each ID denotes a distinct (table, index column), even for equal SQL values.
namespace seron_range {
enum Column : size_t {
    CheckingId, SavingsId, WarehouseId, DistrictWarehouse, DistrictId,
    CustomerWarehouse, CustomerDistrict, CustomerId, StockWarehouse, StockItem,
    ItemId, OrdersWarehouse, OrdersDistrict, NewOrderWarehouse, NewOrderDistrict,
    OrderLineWarehouse, OrderLineDistrict, Count
};
struct Predicate { size_t column; uint64_t value; };
struct ColumnSpec { std::string table, column; uint64_t max_value; };
inline std::vector<ColumnSpec> specs(uint64_t accounts, uint64_t warehouses,
                                     uint64_t customers, uint64_t items) {
    return {{"checking", "id", accounts}, {"savings", "id", accounts},
        {"warehouse", "w_id", warehouses}, {"district", "d_w_id", warehouses},
        {"district", "d_id", 10}, {"customer", "c_w_id", warehouses},
        {"customer", "c_d_id", 10}, {"customer", "c_id", customers},
        {"stock", "s_w_id", warehouses}, {"stock", "s_i_id", items},
        {"item", "i_id", items}, {"orders", "o_w_id", warehouses},
        {"orders", "o_d_id", 10}, {"new_order", "no_w_id", warehouses},
        {"new_order", "no_d_id", 10}, {"order_line", "ol_w_id", warehouses},
        {"order_line", "ol_d_id", 10}};
}
template<class Values>
std::vector<Predicate> smallbank(int type, const Values& accounts) {
    std::vector<Predicate> p;
    auto add = [&](size_t c, size_t i) { p.push_back({c, static_cast<uint64_t>(accounts.at(i))}); };
    switch (type) {
        case 0: add(CheckingId, 0); add(SavingsId, 0); add(CheckingId, 1); break;
        case 1: add(CheckingId, 0); add(CheckingId, 1); break;
        case 2: add(CheckingId, 0); break;
        case 3: case 4: add(CheckingId, 0); add(SavingsId, 0); break;
        case 5: add(SavingsId, 0); break;
        case 6: for (size_t i = 0; i < accounts.size(); ++i) add(CheckingId, i); break;
        default: throw std::invalid_argument("Unknown SmallBank template");
    }
    return p;
}
template<class Values>
std::vector<Predicate> tpcc(int type, const Values& args, bool standard, bool client_order_id) {
    std::vector<Predicate> p;
    auto add = [&](size_t c, uint64_t v) { p.push_back({c, v}); };
    auto wd = [&](size_t w, size_t d) { add(w, args.at(0)); add(d, args.at(1)); };
    auto customer = [&] { wd(CustomerWarehouse, CustomerDistrict); add(CustomerId, args.at(2)); };
    switch (type) {
        case 0: {
            wd(DistrictWarehouse, DistrictId);
            const size_t count_pos = client_order_id ? 4 : 3;
            const auto count = static_cast<size_t>(args.at(count_pos));
            const size_t base = count_pos + 1;
            if (count > 15 || args.size() < base + count * 3)
                throw std::invalid_argument("Malformed NewOrder parameters");
            for (size_t i = 0; i < count; ++i) {
                add(StockItem, args.at(base + 3*i));
                add(StockWarehouse, args.at(base + 3*i + 1));
                if (standard) add(ItemId, args.at(base + 3*i));
            }
            // INSERT values are not lookup predicates. In particular, do not
            // invent customer/warehouse reads absent from our SQL procedure.
            break;
        }
        case 1: add(WarehouseId, args.at(0)); wd(DistrictWarehouse, DistrictId); customer(); break;
        case 2:
            customer(); wd(OrdersWarehouse, OrdersDistrict); wd(OrderLineWarehouse, OrderLineDistrict);
            // The order ID is obtained by a query during execution, not known here.
            break;
        case 3:
            add(WarehouseId, args.at(0));
            for (uint64_t d = 1; d <= 10; ++d) {
                for (auto c : {NewOrderWarehouse, OrdersWarehouse, OrderLineWarehouse, CustomerWarehouse})
                    add(c, args.at(0));
                for (auto c : {NewOrderDistrict, OrdersDistrict, OrderLineDistrict, CustomerDistrict}) add(c, d);
            }
            break;
        case 4:
            wd(DistrictWarehouse, DistrictId); wd(OrderLineWarehouse, OrderLineDistrict);
            add(StockWarehouse, args.at(0));
            // Stock item IDs and the order-ID interval depend on query results.
            break;
        default: throw std::invalid_argument("Unknown TPC-C template");
    }
    return p;
}
} // namespace seron_range
