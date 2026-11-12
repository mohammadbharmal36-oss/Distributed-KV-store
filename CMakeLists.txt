cmake_minimum_required(VERSION 3.20)
project(distributed_kv_store VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic -pthread)

find_package(Threads REQUIRED)

# Core static library shared across server node, CLI client, and test suites
add_library(kv_core STATIC
  src/protocol.cpp
  src/storage.cpp
  src/wal.cpp
  src/network.cpp
  src/server.cpp
  src/client.cpp
  src/replication.cpp
  src/raft.cpp
  src/node.cpp
)

target_include_directories(kv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(kv_core PUBLIC Threads::Threads)

# Server node executable
add_executable(kv_node src/main.cpp)
target_link_libraries(kv_node PRIVATE kv_core)

# Interactive CLI & Benchmark client executable
add_executable(kv_cli src/client_main.cpp)
target_link_libraries(kv_cli PRIVATE kv_core)

# Unit & Integration Tests with GoogleTest
option(BUILD_TESTS "Build GoogleTest unit and integration test suites" ON)
if(BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()

  add_executable(kv_tests
    tests/storage_tests.cpp
    tests/wal_tests.cpp
    tests/network_tests.cpp
    tests/replication_tests.cpp
    tests/raft_tests.cpp
  )
  target_link_libraries(kv_tests PRIVATE kv_core GTest::gtest_main)

  include(GoogleTest)
  gtest_discover_tests(kv_tests)
endif()
