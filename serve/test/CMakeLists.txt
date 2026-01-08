# add_executable(parse_test parse_test.cc)
# target_link_libraries(parse_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})

include_directories(./)

# add_executable(queryplan_test queryplan_cardinality_test.cc)
# target_link_libraries(queryplan_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(transfer_test transfer_sql.cc)
# target_link_libraries(transfer_test ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(pageinspect_test test_pageinspect.cc)
# target_link_libraries(pageinspect_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport) 


add_executable(run run.cc smallbank.cc ycsb.cc tpcc.cc smart_router.cc friend_simulate.cc parse.cc)
target_link_libraries(run global mlp metis logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

add_executable(indexed_priority_queue_test indexed_priority_queue_test.cc)
target_link_libraries(indexed_priority_queue_test)
# add_executable(btree_search btree_search.cc)
# target_link_libraries(btree_search global logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

# add_executable(build_graph_test build_graph_test.cc)
# target_link_libraries(build_graph_test global logger ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY})

# add_executable(metis_performance_test metis_performance_test.cc)
# target_link_libraries(metis_performance_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})
