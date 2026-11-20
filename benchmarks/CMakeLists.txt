option(PULSE_IPC_BENCH_NATIVE "Build benchmark targets for local CPU" OFF)

if(PULSE_IPC_BENCH_NATIVE)
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-march=native" PULSE_IPC_HAS_MARCH_NATIVE)
    if(NOT PULSE_IPC_HAS_MARCH_NATIVE)
        message(FATAL_ERROR "PULSE_IPC_BENCH_NATIVE requires -march=native support")
    endif()
endif()

find_package(Threads REQUIRED)

function(pulse_ipc_add_benchmark module_name benchmark_name)
    add_executable(${benchmark_name} ${module_name}/${benchmark_name}.cpp)
    target_link_libraries(${benchmark_name}
        PRIVATE
            pulse_ipc
            benchmark::benchmark
            Threads::Threads
    )
    if(PULSE_IPC_BENCH_NATIVE)
        target_compile_options(${benchmark_name} PRIVATE -march=native)
    endif()
endfunction()

pulse_ipc_add_benchmark(spsc_queue spsc_queue_throughput)
pulse_ipc_add_benchmark(spsc_queue spsc_queue_rtt)
