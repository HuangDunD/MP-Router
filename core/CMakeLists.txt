include_directories(metis)

set(Metis_SRC 
        metis/partition.cc)

add_library(core STATIC
            ${Metis_SRC})

target_link_libraries(core ${METIS_LIBRARIES})
        