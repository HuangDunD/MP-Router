cmake_minimum_required(VERSION 3.10)

project(MP-Router)

option(LINK_SO "Whether are linked dynamically" ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

set(CMAKE_CXX_STANDARD 17)

include(FindThreads)

# 在用户目录下寻找 metis.h
find_path(METIS_INCLUDE_DIR metis.h
    PATHS
    $ENV{HOME}/local/include/
    PATH_SUFFIXES include
)
if (METIS_INCLUDE_DIR)
    message(STATUS "Found METIS includes: ${METIS_INCLUDE_DIR}")
    include_directories(${METIS_INCLUDE_DIR})
else()
    message(FATAL_ERROR "METIS not found in user directory!")
endif()

# 查找 metis 库
find_library(METIS_LIBRARY
    NAMES metis libmetis
    PATHS
    $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (METIS_LIBRARY) 
    message(STATUS "Found METIS library: ${METIS_LIBRARY}")
else()
    message(FATAL_ERROR "METIS library not found in user directory!")
endif()

# 查找 GKlib 库
find_library(GKLIB_LIBRARY
    NAMES GKlib libGKlib
    PATHS
    $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (GKLIB_LIBRARY) 
    message(STATUS "Found GKlib library: ${GKLIB_LIBRARY}")
else()
    message(FATAL_ERROR "GKlib library not found in user directory!")
endif()

include_directories(./
        core
        thirdparty
        workload
        serve  # 添加serve目录以包含serve.cpp的头文件（如果有）
)

set(CONFIG_SRC config.cc)
add_library(global STATIC ${CONFIG_SRC})


add_subdirectory(core)

add_subdirectory(workload)

add_subdirectory(serve)