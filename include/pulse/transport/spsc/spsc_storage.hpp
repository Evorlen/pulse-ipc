// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/config/config.hpp"
#include "pulse/storage/slot.hpp"

namespace pulse::transport::spsc
{

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue;

static_assert(
    std::atomic<std::uint64_t>::is_always_lock_free,
    "std::atomic<std::uint64_t> must be always lock-free."
);

// Cache line for the producer and consumer.
struct alignas(config::cache_line_alignment) spsc_queue_cache_line final
{
    std::atomic<std::uint64_t> seq {0};
};

static_assert(
    sizeof(spsc_queue_cache_line) == config::cache_line_alignment,
    "the size of spsc_queue_cache_line must be equal to cache_line_alignment."
);

// Storage for the SPSC queue.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_storage final
{
private:
    friend class spsc_queue<T, Capacity>;

    static constexpr std::size_t slots_alignment = alignof(T) > config::cache_line_alignment
                                                       ? alignof(T)
                                                       : config::cache_line_alignment;
    static constexpr std::size_t physical_capacity = std::bit_ceil(Capacity);

public:
    spsc_queue_storage(const spsc_queue_storage &) = delete;

    auto operator=(const spsc_queue_storage &) -> spsc_queue_storage & = delete;

    spsc_queue_storage(spsc_queue_storage &&) = delete;

    auto operator=(spsc_queue_storage &&) -> spsc_queue_storage & = delete;

private:
    spsc_queue_storage() noexcept = default;

public:
    // Return the producer cache line.
    [[nodiscard]]
    auto producer_cache_line() noexcept -> spsc_queue_cache_line &
    {
        return producer_cache_line_;
    }

    // Return the producer cache line.
    [[nodiscard]]
    auto producer_cache_line() const noexcept -> const spsc_queue_cache_line &
    {
        return producer_cache_line_;
    }

    // Return the consumer cache line.
    [[nodiscard]]
    auto consumer_cache_line() noexcept -> spsc_queue_cache_line &
    {
        return consumer_cache_line_;
    }

    // Return the consumer cache line.
    [[nodiscard]]
    auto consumer_cache_line() const noexcept -> const spsc_queue_cache_line &
    {
        return consumer_cache_line_;
    }

    // Return the slot at the given index. The caller must ensure that the index is valid.
    [[nodiscard]]
    auto slot(std::size_t index) noexcept -> storage::spsc_queue_slot<T> &
    {
        return slots_[index];
    }

private:
    spsc_queue_cache_line producer_cache_line_;
    spsc_queue_cache_line consumer_cache_line_;
    alignas(slots_alignment) std::array<storage::spsc_queue_slot<T>, physical_capacity> slots_;
};

} // namespace pulse::transport::spsc
