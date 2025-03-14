include_directories(metis)

set(Metis_SRC 
        metis/partition.cc)

add_library(core STATIC
            ${Metis_SRC})

target_link_libraries(core ${METIS_LIBRARY} ${GKLIB_LIBRARY})
        

set(Metis_test_SRC 
        clump.cc)

add_executable(clump ${Metis_test_SRC})

target_link_libraries(clump core global)