// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include "pulse/memory/mapped_region.hpp"

#include <utility>

#if defined(__linux__)
#include <sys/mman.h>
#else
#error "Unsupported platform"
#endif

namespace pulse::memory
{

mapped_region::mapped_region(mapped_region && other) noexcept
    : address_(std::exchange(other.address_, nullptr))
    , size_(std::exchange(other.size_, std::size_t {0}))
{}

auto mapped_region::operator=(mapped_region && other) noexcept -> mapped_region &
{
    if (this != &other) {
        mapped_region incoming(std::move(other));
        std::swap(address_, incoming.address_);
        std::swap(size_, incoming.size_);
    }

    return *this;
}

mapped_region::~mapped_region() noexcept
{
    if (size_ == 0) {
        return;
    }

#if defined(__linux__)
    static_cast<void>(::munmap(address_, size_));
#else
#error "Unsupported platform"
#endif
}

mapped_region::mapped_region(void * address, std::size_t size) noexcept
    : address_(address)
    , size_(size)
{}

auto mapped_region::bytes() noexcept -> std::span<std::byte>
{
    return {static_cast<std::byte *>(address_), size_};
}

auto mapped_region::bytes() const noexcept -> std::span<const std::byte>
{
    return {static_cast<const std::byte *>(address_), size_};
}

} // namespace pulse::memory
