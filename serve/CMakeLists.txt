set(SERVER_SRC serve.cpp)
add_executable(serve ${SERVER_SRC})
add_executable(offline_analyzer offline_analyze.cc)

target_link_libraries(serve global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY})
target_link_libraries(offline_analyzer global ${METIS_LIBRARY} ${GKLIB_LIBRARY} ${PQXX_LIBRARY} ${PQ_LIBRARY})

add_subdirectory(test)