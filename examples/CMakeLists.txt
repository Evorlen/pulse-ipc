function(pulse_ipc_add_example module_name example_name)
    add_executable(${example_name} ${module_name}/${example_name}.cpp)
    target_link_libraries(${example_name} PRIVATE pulse_ipc)
endfunction()

pulse_ipc_add_example(spsc_queue spsc_queue)
pulse_ipc_add_example(spsc_channel spsc_channel)

find_package(Threads REQUIRED)
target_link_libraries(spsc_queue PRIVATE Threads::Threads)
