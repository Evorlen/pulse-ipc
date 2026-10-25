// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/config/config.hpp"

namespace pulse::spsc
{

// Cache line for the producer and consumer.
// For the producer, seq is write_seq and cached_seq is read_seq.
// For the consumer, seq is read_seq and cached_seq is write_seq.
struct alignas(config::cache_line_alignment) spsc_queue_cache_line final
{
    std::atomic<std::uint64_t> seq {0};
    std::uint64_t cached_seq {0};
};

static_assert(
    sizeof(spsc_queue_cache_line) <= config::cache_line_alignment,
    "the size of spsc_queue_cache_line must be less than or equal to cache_line_alignment."
);

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue final
{
private:
    struct alignas(T) spsc_queue_slot final
    {
        std::array<std::byte, sizeof(T)> storage;
    };

public:
    spsc_queue() = default;

    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

    spsc_queue(spsc_queue &&) = delete;

    auto operator=(spsc_queue &&) -> spsc_queue & = delete;

    ~spsc_queue() noexcept
    {
        auto read_seq = consumer_cache_line_.seq.load(std::memory_order_relaxed);
        const auto write_seq = producer_cache_line_.seq.load(std::memory_order_relaxed);

        // Destroy only the live elements that have not been consumed.
        while (read_seq != write_seq) {
            auto * slot = std::launder(
                reinterpret_cast<T *>(slots_[read_seq & (Capacity - 1)].storage.data())
            );
            std::destroy_at(slot);
            ++read_seq;
        }
    }

public:
    // Copy a T value into the queue.
    [[nodiscard]]
    auto try_push(const T & value) noexcept(std::is_nothrow_copy_constructible_v<T>) -> bool
        requires std::is_copy_constructible_v<T>
    {
        return _try_emplace(value);
    }

    // Move a T value into the queue.
    [[nodiscard]]
    auto try_push(T && value) noexcept(std::is_nothrow_move_constructible_v<T>) -> bool
        requires std::is_move_constructible_v<T>
    {
        return _try_emplace(std::move(value));
    }

    // Construct a T value directly in the queue.
    template <typename... Args>
    [[nodiscard]]
    auto try_emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> bool
        requires std::is_constructible_v<T, Args...>
    {
        return _try_emplace(std::forward<Args>(args)...);
    }

    // Copy a T value out of the queue.
    [[nodiscard]]
    auto try_pop(T & value) noexcept(std::is_nothrow_copy_assignable_v<T>) -> bool
        requires std::is_copy_assignable_v<T>
    {
        return _try_consume(
            [&value](const T & element) noexcept(std::is_nothrow_copy_assignable_v<T>) -> void {
                value = element;
            }
        );
    }

    // Move a T value out of the queue.
    [[nodiscard]]
    auto try_pop_move(T & value) noexcept(std::is_nothrow_move_assignable_v<T>) -> bool
        requires std::is_move_assignable_v<T>
    {
        return _try_consume(
            [&value](T & element) noexcept(std::is_nothrow_move_assignable_v<T>) -> void {
                value = std::move(element);
            }
        );
    }

    // Invoke the consumer on the front element, then destroy it and release the slot.
    // Return false without invoking the consumer if the queue is empty; otherwise return true.
    // The consumer's return value is ignored.
    // If the consumer throws, the element stays queued but may have been modified.
    // The consumer must not retain element references beyond the call or reenter consumption.
    template <typename Consumer>
    [[nodiscard]]
    auto try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>)
        -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        return _try_consume(std::forward<Consumer>(consume));
    }

    // Return an approximate observation of whether the queue is full.
    // Concurrent operations may change the state before this call returns.
    [[nodiscard]]
    auto full() const noexcept -> bool
    {
        const auto write_seq = producer_cache_line_.seq.load(std::memory_order_acquire);
        const auto read_seq = consumer_cache_line_.seq.load(std::memory_order_acquire);
        return write_seq - read_seq == Capacity;
    }

    // Return an approximate observation of whether the queue is empty.
    // Concurrent operations may change the state before this call returns.
    [[nodiscard]]
    auto empty() const noexcept -> bool
    {
        const auto read_seq = consumer_cache_line_.seq.load(std::memory_order_acquire);
        const auto write_seq = producer_cache_line_.seq.load(std::memory_order_acquire);
        return read_seq == write_seq;
    }

    // Return the queue's capacity.
    [[nodiscard]]
    static constexpr auto capacity() noexcept -> std::size_t
    {
        return Capacity;
    }

private:
    template <typename... Args>
    auto _try_emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> bool
        requires std::is_constructible_v<T, Args...>
    {
        // Load data from producer's cache line.
        const auto write_seq = producer_cache_line_.seq.load(std::memory_order_relaxed);
        auto & cached_read_seq = producer_cache_line_.cached_seq;

        // Check if the queue is full.
        if (write_seq - cached_read_seq == Capacity) {
            // Slow path. Read and update the producer's cached read sequence.
            cached_read_seq = consumer_cache_line_.seq.load(std::memory_order_acquire);

            // Check if the queue is still full.
            if (write_seq - cached_read_seq == Capacity) {
                return false;
            }
        }

        // Fast path.
        // Construct the element in the aligned slot storage.
        auto * slot = reinterpret_cast<T *>(slots_[write_seq & (Capacity - 1)].storage.data());
        std::construct_at(slot, std::forward<Args>(args)...);

        // Publish the element only after construction succeeds.
        producer_cache_line_.seq.store(write_seq + 1, std::memory_order_release);
        return true;
    }

    template <typename Consumer>
    [[nodiscard]]
    auto _try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>)
        -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        // Load data from consumer's cache line.
        const auto read_seq = consumer_cache_line_.seq.load(std::memory_order_relaxed);
        auto & cached_write_seq = consumer_cache_line_.cached_seq;

        // Check if the queue is empty.
        if (read_seq == cached_write_seq) {
            // Slow path. Read and update the consumer's cached write sequence.
            cached_write_seq = producer_cache_line_.seq.load(std::memory_order_acquire);

            // Check if the queue is still empty.
            if (read_seq == cached_write_seq) {
                return false;
            }
        }

        // Fast path.
        // Consume the element, then destroy it in the slot.
        auto * slot =
            std::launder(reinterpret_cast<T *>(slots_[read_seq & (Capacity - 1)].storage.data()));
        std::invoke(std::forward<Consumer>(consume), *slot);
        std::destroy_at(slot);

        // Release the slot only after consumption and destruction succeed.
        consumer_cache_line_.seq.store(read_seq + 1, std::memory_order_release);
        return true;
    }

private:
    spsc_queue_cache_line producer_cache_line_;
    spsc_queue_cache_line consumer_cache_line_;
    std::array<spsc_queue_slot, Capacity> slots_;
};

} // namespace pulse::spsc
