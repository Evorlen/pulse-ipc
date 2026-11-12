// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <span>

namespace pulse::memory
{

class mapped_region final
{
private:
    friend class shared_memory;

public:
    mapped_region() noexcept = default;

    mapped_region(const mapped_region &) = delete;

    auto operator=(const mapped_region &) -> mapped_region & = delete;

    mapped_region(mapped_region &&) noexcept;

    auto operator=(mapped_region &&) noexcept -> mapped_region &;

    ~mapped_region() noexcept;

private:
    explicit mapped_region(void * address, std::size_t size) noexcept;

public:
    [[nodiscard]]
    auto bytes() noexcept -> std::span<std::byte>;

    [[nodiscard]]
    auto bytes() const noexcept -> std::span<const std::byte>;

private:
    void * address_ {nullptr};
    std::size_t size_ {0};
};

} // namespace pulse::memory
