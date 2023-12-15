cmake_minimum_required(VERSION 3.11)
project(pear_tree)

set(CMAKE_CXX_STANDARD 20)


option(LIB_TYP "Set to r-tree variant identifier")
if (NOT ${LIB_TYP})
    set(LIB_TYP basic)
endif ()

option(PMEM_INSTRUCTIONS "Use intel optane specific instruction set")
if(PMEM_INSTRUCTIONS)
    add_compile_definitions(USE_PMEM_INSTRUCTIONS)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mclwb -mavx2 -pthread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mclwb -mavx2 -pthread -mavx512f -mavx512vl -mavx512dq -msse -msse2")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

set(LOCAL_INCLUDE src/lib src/utils src/basic src/pear src/fpr-tree)
include_directories(${LOCAL_INCLUDE})
##################### PMDK ####################
option(BUILD_PMDK "Set true if PMDK should be downloaded and built from source.")
option(PMDK_INCLUDE_PATH "Path to custom PMDK include files" "")
option(PMDK_LIBRARY_PATH "Path to custom PMDK library" "")
if (${BUILD_PMDK})
    message(STATUS "PMDK not provided. Downloading and building from source.")
    set(PMDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmdk")
    ExternalProject_Add(
            PMDK
            GIT_REPOSITORY https://github.com/pmem/pmdk.git
            GIT_TAG 1.10
            BUILD_IN_SOURCE 1
            BUILD_COMMAND $(MAKE)
            PREFIX ${PMDK_PREFIX}
            CONFIGURE_COMMAND ""
            INSTALL_COMMAND ""
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            UPDATE_DISCONNECTED 1
    )
    include_directories(${PMDK_PREFIX}/src/PMDK/src/include)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        link_directories(${PMDK_PREFIX}/src/PMDK/src/debug)
    else ()
        link_directories(${PMDK_PREFIX}/src/PMDK/src/nondebug)
    endif ()
else ()
    set(CMAKE_INCLUDE_PATH ${PMDK_INCLUDE_PATH} /usr/local/include)
    set(PMDK_HINTS ${PMDK_LIBRARY_PATH}  /scratch/pmem/pmdk/lib /usr/local/lib64 /usr/local/lib)
    find_path(PMDK_INCLUDE_DIRS libpmem2.h REQUIRED HINTS /scratch/pmem/pmdk/include)
    set(PMEM_INCLUDE_DIRS ${PMDK_INCLUDE_DIRS}) # ${MEMKIND_INCLUDE_DIRS})
    find_library(PMDK_LIBRARIES NAMES pmem2 libpmem2 REQUIRED HINTS ${PMDK_HINTS})
    set(PMEM_LIBRARIES ${PMDK_LIBRARIES} ${MEMKIND_LIBRARIES})
    if (NOT PMEM_INCLUDE_DIRS OR "${PMEM_INCLUDE_DIRS}" STREQUAL "")
        message(FATAL_ERROR "ERROR: libpmem2 include directory not found.")
    endif ()
    if (NOT PMEM_LIBRARIES OR "${PMEM_LIBRARIES}" STREQUAL "")
        message(FATAL_ERROR "ERROR: libpmem2 library not found.")
    endif ()
    include_directories(${PMEM_INCLUDE_DIRS})
    message(STATUS "PMDK provided. Including ${PMEM_INCLUDE_DIRS} and linking ${PMEM_LIBRARIES}.")
endif ()
##################### END PMDK ####################




set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
if(EXISTS "/hpi/fs00/home/nils.thamm/boost_1_75_0/")
    set(Boost_INCLUDE_DIR "/hpi/fs00/home/nils.thamm/boost_1_75_0")
endif()
find_package(Boost)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()


include(FetchContent)
# GOOGLE BENCHMARK
FetchContent_Declare(
        google_benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.5.2
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
FetchContent_MakeAvailable(google_benchmark)
include_directories(${google_benchmark_SOURCE_DIR}/include)
message(STATUS "Google Benchmark include dir ${google_benchmark_SOURCE_DIR}/include/")

add_subdirectory(src)
