add_executable(parse_test parse_test.cc)
target_link_libraries(parse_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY})

add_executable(queryplan_test queryplan_cardinality_test.cc)
target_link_libraries(queryplan_test global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY} pgcommon pgport)