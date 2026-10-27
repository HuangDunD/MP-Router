# add_executable(parse_test parse_test.cc)
# target_link_libraries(parse_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})

include_directories(./)

# add_executable(queryplan_test queryplan_cardinality_test.cc)
# target_link_libraries(queryplan_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(transfer_test transfer_sql.cc)
# target_link_libraries(transfer_test ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(pageinspect_test test_pageinspect.cc)
# target_link_libraries(pageinspect_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport) 

add_executable(yashan_test yashan_test.cc)
target_link_libraries(yashan_test ${YS_LIBRARY} )

set(RUN_SOURCES run.cc smallbank.cc ycsb.cc tpcc.cc smart_router.cc friend_simulate.cc parse.cc)
if (MYSQL_INCLUDE_DIR AND MYSQLCLIENT_LIBRARY)
    list(APPEND RUN_SOURCES mysql_workload.cc)
endif()

add_executable(run ${RUN_SOURCES})
target_link_libraries(run global mlp metis logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} ${YS_LIBRARY} pgcommon pgport)
if (MYSQL_INCLUDE_DIR AND MYSQLCLIENT_LIBRARY)
    target_link_libraries(run ${MYSQLCLIENT_LIBRARY})
endif()

add_executable(indexed_priority_queue_test indexed_priority_queue_test.cc)
target_link_libraries(indexed_priority_queue_test)

add_executable(seron_page_conflict_test seron_page_conflict_test.cc)
add_executable(seron_range_rules_test seron_range_rules_test.cc)
target_link_libraries(seron_range_rules_test Threads::Threads)
add_executable(seron_column_rules_test seron_column_rules_test.cc)
target_link_libraries(seron_column_rules_test Threads::Threads)
target_link_libraries(seron_page_conflict_test Threads::Threads)

add_executable(seron_connector_queue_test seron_connector_queue_test.cc ${PROJECT_SOURCE_DIR}/config.cc)
target_link_libraries(seron_connector_queue_test logger Threads::Threads)
# add_executable(btree_search btree_search.cc)
# target_link_libraries(btree_search global logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(build_graph_test build_graph_test.cc)
# target_link_libraries(build_graph_test global logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY})

# add_executable(metis_performance_test metis_performance_test.cc)
# target_link_libraries(metis_performance_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})
