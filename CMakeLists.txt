cmake_minimum_required(VERSION 3.27)

project(
    PULSE-IPC
    VERSION 0.1.0
    DESCRIPTION "High-performance shared-memory IPC with predictable latency and crash-aware recovery."
    LANGUAGES CXX
)

# Set C++ standard to C++23.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Disable C++ extensions.
set(CMAKE_CXX_EXTENSIONS OFF)

# This project does not currently use C++ modules.
# Disable automatic module dependency scanning on CMake 3.28 and newer.
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

# Enable export of compile commands for clang-tidy and other tools.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PULSE_IPC_TEST "Build test targets" OFF)
option(PULSE_IPC_BENCH "Build benchmark targets" OFF)
option(PULSE_IPC_EXAMPLE "Build example targets" OFF)

include(FetchContent)

if(PULSE_IPC_TEST)
    enable_testing()

    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.18.0
    )

    if(MSVC)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()

    FetchContent_MakeAvailable(googletest)

    # add_subdirectory(tests)
endif()

if(PULSE_IPC_BENCH)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.10.0
    )

    FetchContent_MakeAvailable(benchmark)

    # add_subdirectory(benchmarks)
endif()
