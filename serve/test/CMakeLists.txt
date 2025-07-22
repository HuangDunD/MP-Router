# add_executable(parse_test parse_test.cc)
# target_link_libraries(parse_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})

add_executable(queryplan_test queryplan_cardinality_test.cc)
target_link_libraries(queryplan_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

add_executable(transfer_test transfer_sql.cc)
target_link_libraries(transfer_test ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)

add_executable(pageinspect_test test_pageinspect.cc)
target_link_libraries(pageinspect_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport) 

add_executable(smallbank_test smallbank.cc)
target_link_libraries(smallbank_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)