add_library(pulse_ipc STATIC)

target_sources(pulse_ipc
    PRIVATE
        memory/mapped_region.cpp
        memory/shared_memory.cpp
        sync/initialization_lock.cpp
)

target_include_directories(pulse_ipc
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(pulse_ipc PUBLIC cxx_std_23)

add_library(pulse::ipc ALIAS pulse_ipc)
