// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/config/config.hpp"
#include "pulse/spsc/spsc_storage.hpp"

namespace pulse::spsc
{

// SPSC Queue Producer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class alignas(config::cache_line_alignment) spsc_queue_producer final
{
private:
    friend class spsc_queue<T, Capacity>;
    using storage_type = spsc_queue_storage<T, Capacity>;
    using slot_type = spsc_queue_slot<T>;

public:
    spsc_queue_producer(const spsc_queue_producer &) = delete;

    auto operator=(const spsc_queue_producer &) -> spsc_queue_producer & = delete;

    spsc_queue_producer(spsc_queue_producer &&) = delete;

    auto operator=(spsc_queue_producer &&) -> spsc_queue_producer & = delete;

private:
    explicit spsc_queue_producer(storage_type & storage) noexcept
        : producer_cache_line_(&storage.producer_cache_line())
        , consumer_cache_line_(&storage.consumer_cache_line())
        , slots_(&storage.slot(0))
        , local_write_seq_(producer_cache_line_->seq.load(std::memory_order_relaxed))
        , cached_write_limit_(consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity)
    {}

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

    // Reserve raw storage for one T, or return nullptr when full.
    // Repeated calls return the same reservation. Construct a T before commit().
    // The pointer expires at commit/cancel; endpoint operations must not be reentered.
    [[nodiscard]]
    auto try_reserve() noexcept -> T *
    {
        // The producer owns its local write sequence; only refresh the peer's sequence when full.
        const auto write_seq = local_write_seq_;

        // Check if the queue is full.
        if (write_seq == cached_write_limit_) {
            // Slow path. Read and update the producer's cached read sequence.
            cached_write_limit_ =
                consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity;

            // Check if the queue is still full.
            if (write_seq == cached_write_limit_) {
                return nullptr;
            }
        }

        return slots_[write_seq & (Capacity - 1)].address();
    }

    // Requires a successful reservation containing a live T. Call exactly once.
    // The caller must not mix ordinary pushes with an outstanding reservation.
    auto commit() noexcept -> void
    {
        ++local_write_seq_;
        producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);
    }

    // No-op: reservations do not change queue state.
    // Caller must destroy any constructed T first. Raw storage has no known lifetime.
    // Resolve reservations before retiring the endpoint; its destructor does not clean them up.
    auto cancel() noexcept -> void
    {
        // Do nothing.
    }

private:
    template <typename... Args>
    auto _try_emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> bool
        requires std::is_constructible_v<T, Args...>
    {
        // The producer owns its local write sequence; only refresh the peer's sequence when full.
        const auto write_seq = local_write_seq_;

        // Check if the queue is full.
        if (write_seq == cached_write_limit_) {
            // Slow path. Read and update the producer's cached read sequence.
            cached_write_limit_ =
                consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity;

            // Check if the queue is still full.
            if (write_seq == cached_write_limit_) {
                return false;
            }
        }

        auto * slot = slots_[write_seq & (Capacity - 1)].address();
        std::construct_at(slot, std::forward<Args>(args)...);

        // Publish only after construction succeeds; exceptions leave the sequence unchanged.
        local_write_seq_ = write_seq + 1;
        producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);

        return true;
    }

private:
    spsc_queue_cache_line * producer_cache_line_;
    spsc_queue_cache_line * consumer_cache_line_;
    slot_type * slots_;
    std::uint64_t local_write_seq_;
    std::uint64_t cached_write_limit_;
};

} // namespace pulse::spsc
