set(SERVER_SRC serve.cpp)
add_executable(serve ${SERVER_SRC})

target_link_libraries(serve ${METIS_LIBRARY} ${GKLIB_LIBRARY})