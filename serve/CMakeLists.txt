include_directories(log)
include_directories(region)
# --- MODIFICATION START ---
# List all source files needed for the 'serve' executable
set(LOG_SRC
        log/Logger.cpp
)

set(METIS_SRC
        metis_partitioner.cc
)

set(SERVER_SRC
        serve.cpp
        region/region_generator.cpp
        # Add any other .cpp files needed directly by 'serve' here
)
# --- MODIFICATION END ---

# add_executable(serve ${SERVER_SRC} ${LOG_SRC})
add_executable(offline_analyzer offline_analyze.cc)
add_executable(create_table create_table.cc)

add_library(metis STATIC ${METIS_SRC})
add_library(logger STATIC ${LOG_SRC})

# Link libraries to 'serve'
# target_link_libraries(serve PRIVATE # Use PRIVATE or PUBLIC as appropriate
#         global # Assuming 'global' is a library target defined elsewhere
#         # nlohmann_json::nlohmann_json
#         ${METIS_LIBRARY}
#         ${GKLIB_LIBRARY}
#         ${PQXX_LIBRARY}
#         ${PQ_LIBRARY}
#         pgcommon 
#         pgport
# )

# Link libraries to 'offline_analyzer' (if it also needs Logger, Region etc., add sources above too)
target_link_libraries(offline_analyzer PRIVATE # Use PRIVATE or PUBLIC
        global
        ${PQXX_LIBRARY}
        ${PQ_LIBRARY}
        pgcommon
        pgport
)

target_link_libraries(create_table PRIVATE # Use PRIVATE or PUBLIC
        global
        ${PQXX_LIBRARY}
        ${PQ_LIBRARY}
        pgcommon
        pgport

)

add_subdirectory(test)

# Optional: If 'global' library already includes Logger and RegionProcessor objects,
# you might only need to link against global. But based on the errors,
# it seems 'serve' needs to compile these sources directly or link another library
# target that contains them. Adding them to SERVER_SRC is the direct approach.