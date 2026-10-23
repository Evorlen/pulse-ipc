// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "pulse/config.hpp"

namespace pulse
{

// Cache line for the producer and consumer.
// For the producer, seq is write_seq and cached_index is read_index.
// For the consumer, seq is read_seq and cached_index is write_index.
struct alignas(cache_line_alignment) spsc_queue_cache_line final
{
    std::atomic<std::uint64_t> seq {0};
    std::uint64_t cached_index {0};
};

static_assert(
    sizeof(spsc_queue_cache_line) <= cache_line_alignment,
    "spsc_queue_cache_line must be less than or equal to cache line alignment."
);

// An in-progress, single-producer, single-consumer queue with fixed capacity.
template <typename T, std::size_t Capacity>
class spsc_queue final
{
    static_assert(
        std::atomic<std::uint64_t>::is_always_lock_free,
        "std::uint64_t must be always lock-free."
    );
    static_assert(std::is_default_constructible_v<T>, "T must be default constructible.");
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable.");
    static_assert(std::is_trivially_copy_assignable_v<T>, "T must be trivially copy assignable.");
    static_assert(Capacity >= 2, "Capacity must greater than or equal to 2.");
    static_assert(std::has_single_bit(Capacity), "Capacity must be a power of 2.");

public:
    spsc_queue() = default;

    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

public:
    [[nodiscard]]
    auto try_push(const T & value) noexcept -> bool
    {
        const auto write_seq = producer_cache_line_.seq.load(std::memory_order_relaxed);
        auto & cached_read_index = producer_cache_line_.cached_index;

        // Check if the queue is full.
        if (write_seq - cached_read_index == Capacity) {
            // Slow path. Read and update the producer's cached read index.
            cached_read_index = consumer_cache_line_.seq.load(std::memory_order_acquire);

            // Check if the queue is still full.
            if (write_seq - cached_read_index == Capacity) {
                return false;
            }
        }

        // Fast path.
        slots_[write_seq & (Capacity - 1)] = value;
        producer_cache_line_.seq.store(write_seq + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]]
    auto try_pop(T & value) noexcept -> bool
    {
        const auto read_seq = consumer_cache_line_.seq.load(std::memory_order_relaxed);
        auto & cached_write_index = consumer_cache_line_.cached_index;

        // Check if the queue is empty.
        if (read_seq == cached_write_index) {
            // Slow path. Read and update the consumer's cached write index.
            cached_write_index = producer_cache_line_.seq.load(std::memory_order_acquire);

            // Check if the queue is still empty.
            if (read_seq == cached_write_index) {
                return false;
            }
        }

        // Fast path.
        // Copy from const T & so that the non-template copy assignment is preferred
        // over a forwarding copy assignment when both are viable.
        value = std::as_const(slots_[read_seq & (Capacity - 1)]);

        consumer_cache_line_.seq.store(read_seq + 1, std::memory_order_release);

        return true;
    }

    [[nodiscard]]
    static constexpr auto capacity() noexcept -> std::size_t
    {
        return Capacity;
    }

private:
    spsc_queue_cache_line producer_cache_line_ {};
    spsc_queue_cache_line consumer_cache_line_ {};
    alignas(cache_line_alignment) std::array<T, Capacity> slots_ {};
};

} // namespace pulse
