# --- MODIFICATION START ---
# List all source files needed for the 'serve' executable
set(SERVER_SRC
        serve.cpp
        region/region_generator.cpp
        log/Logger.cpp
        # Add any other .cpp files needed directly by 'serve' here
)
# --- MODIFICATION END ---

# --- Fetch and Configure nlohmann/json using FetchContent ---
include(FetchContent)
FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        # Specify a version tag for stability (e.g., v3.11.3 or check for latest)
        GIT_TAG v3.11.3
        # If you have it downloaded locally, you could use SOURCE_DIR instead:
        # SOURCE_DIR /path/to/your/local/nlohmann/json
)
# Make the content available (downloads if necessary)
FetchContent_MakeAvailable(nlohmann_json)

add_executable(serve ${SERVER_SRC})
add_executable(offline_analyzer offline_analyze.cc)

# Link libraries to 'serve'
target_link_libraries(serve PRIVATE # Use PRIVATE or PUBLIC as appropriate
        global # Assuming 'global' is a library target defined elsewhere
        nlohmann_json::nlohmann_json
        ${METIS_LIBRARY}
        ${GKLIB_LIBRARY}
        ${PQXX_LIBRARY}
        ${PQ_LIBRARY}
)

# Link libraries to 'offline_analyzer' (if it also needs Logger, Region etc., add sources above too)
target_link_libraries(offline_analyzer PRIVATE # Use PRIVATE or PUBLIC
        global
        ${METIS_LIBRARY}
        ${GKLIB_LIBRARY}
        ${PQXX_LIBRARY}
        ${PQ_LIBRARY}
)

add_subdirectory(test)

# Optional: If 'global' library already includes Logger and RegionProcessor objects,
# you might only need to link against global. But based on the errors,
# it seems 'serve' needs to compile these sources directly or link another library
# target that contains them. Adding them to SERVER_SRC is the direct approach.