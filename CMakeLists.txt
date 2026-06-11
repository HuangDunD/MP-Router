cmake_minimum_required(VERSION 3.10)

project(MP-Router)

# option(LINK_SO "Whether are linked dynamically" ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

set(CMAKE_CXX_STANDARD 20)

# 强制静态链接
# set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
# set(BUILD_SHARED_LIBS OFF)
# set(CMAKE_EXE_LINKER_FLAGS "-static")

include(FindThreads)

include_directories($ENV{HOME}/local/include)
link_directories($ENV{HOME}/local/lib)
link_directories(/usr/lib/postgresql/14/lib)

include_directories(/root/yashandb-client/include)

find_program(MYSQL_CONFIG_EXECUTABLE mysql_config
    PATHS
    /usr/bin
    /usr/local/bin
    /opt/homebrew/bin
)
if (MYSQL_CONFIG_EXECUTABLE)
    execute_process(
        COMMAND ${MYSQL_CONFIG_EXECUTABLE} --include
        OUTPUT_VARIABLE MYSQL_CONFIG_INCLUDE_FLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND ${MYSQL_CONFIG_EXECUTABLE} --libs
        OUTPUT_VARIABLE MYSQL_CONFIG_LIB_FLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    separate_arguments(MYSQL_CONFIG_INCLUDE_FLAGS)
    separate_arguments(MYSQL_CONFIG_LIB_FLAGS)
    foreach(flag ${MYSQL_CONFIG_INCLUDE_FLAGS})
        if (flag MATCHES "^-I(.+)")
            list(APPEND MYSQL_CONFIG_INCLUDE_DIRS "${CMAKE_MATCH_1}")
        endif()
    endforeach()
endif()

set(MYSQL_ROOT "" CACHE PATH "Optional MySQL source/install root")
find_path(MYSQL_INCLUDE_DIR mysql.h
    PATHS
    ${MYSQL_CONFIG_INCLUDE_DIRS}
    ${MYSQL_ROOT}/include
    ${MYSQL_ROOT}/include/mysql
    /usr/include/mysql
    /usr/local/include
    /usr/local/include/mysql
    /opt/homebrew/include
    /opt/homebrew/include/mysql
)
find_library(MYSQLCLIENT_LIBRARY
    NAMES mysqlclient libmysqlclient
    PATHS
    ${MYSQL_ROOT}/lib
    ${MYSQL_ROOT}/lib64
    /usr/lib
    /usr/lib64
    /usr/local/lib
    /opt/homebrew/lib
    PATH_SUFFIXES mysql
)
if (MYSQL_INCLUDE_DIR AND MYSQLCLIENT_LIBRARY)
    message(STATUS "Found MySQL client include: ${MYSQL_INCLUDE_DIR}")
    message(STATUS "Found MySQL client library: ${MYSQLCLIENT_LIBRARY}")
    include_directories(${MYSQL_INCLUDE_DIR})
    add_compile_definitions(WITH_MYSQL_CLIENT=1)
elseif (MYSQL_CONFIG_EXECUTABLE AND MYSQL_CONFIG_INCLUDE_DIRS AND MYSQL_CONFIG_LIB_FLAGS)
    message(STATUS "Using MySQL client flags from mysql_config: ${MYSQL_CONFIG_EXECUTABLE}")
    include_directories(${MYSQL_CONFIG_INCLUDE_DIRS})
    set(MYSQLCLIENT_LIBRARY ${MYSQL_CONFIG_LIB_FLAGS})
    add_compile_definitions(WITH_MYSQL_CLIENT=1)
else()
    message(WARNING "MySQL client library/header not found; DB_TYPE=2 will not be available.")
endif()

# 查找 libpq
find_library(YS_LIBRARY 
    yascli PATHS 
    /root/yashandb-client/lib
    PATH_SUFFIXES lib
)
if (NOT YS_LIBRARY)
    message(FATAL_ERROR "libyascli (YashanDB client) not found!")
else()
    message(STATUS "Found libyascli: ${YS_LIBRARY}")
endif()


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

# 查找 PQXX 库
find_library(PQXX_LIBRARY
    NAMES pqxx libpqxx
    PATHS
    /usr/local/lib
    PATH_SUFFIXES lib
)
if (PQXX_LIBRARY) 
    message(STATUS "Found PQXX library: ${PQXX_LIBRARY}")
else()
    message(FATAL_ERROR "PQXX library not found in /usr/local/lib directory!")
endif()

# 查找 libpq
find_library(PQ_LIBRARY 
    pq PATHS 
    /usr/lib /usr/lib64 /usr/local/lib $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (NOT PQ_LIBRARY)
    message(FATAL_ERROR "libpq (PostgreSQL client) not found!")
else()
    message(STATUS "Found libpq: ${PQ_LIBRARY}")
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

add_subdirectory(serve)
