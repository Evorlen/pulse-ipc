// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <string_view>

#include "pulse/common/result.hpp"
#include "pulse/memory/mapped_region.hpp"

namespace pulse::memory
{

class shared_memory final
{
private:
#if defined(__linux__) || defined(__APPLE__)
    using handle_type = int;
    static constexpr handle_type invalid_fd = -1;
#elif defined(_WIN32)
    using handle_type = void *;
    static constexpr handle_type invalid_handle = nullptr;
#else
#error "Unsupported platform"
#endif

public:
    shared_memory() noexcept = default;

    shared_memory(const shared_memory &) = delete;

    auto operator=(const shared_memory &) -> shared_memory & = delete;

    shared_memory(shared_memory &&) noexcept;

    auto operator=(shared_memory &&) noexcept -> shared_memory &;

    ~shared_memory() noexcept;

private:
    explicit shared_memory(handle_type handle, std::size_t size) noexcept;

public:
    [[nodiscard]]
    static auto create(std::string_view name, std::size_t size) -> common::result<shared_memory>;

    [[nodiscard]]
    static auto open(std::string_view name) -> common::result<shared_memory>;

    [[nodiscard]]
    auto map() const noexcept -> common::result<mapped_region>;

    void close() noexcept;

    [[nodiscard]]
    static auto unlink(std::string_view name) -> common::status;

    [[nodiscard]]
    auto size() const noexcept -> std::size_t;

private:
#if defined(__linux__) || defined(__APPLE__)
    int fd_ {invalid_fd};
#elif defined(_WIN32)
    void * handle_ {invalid_handle};
#else
#error "Unsupported platform"
#endif

    std::size_t size_ {0};
};

} // namespace pulse::memory
