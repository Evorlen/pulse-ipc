// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/spsc/spsc_storage.hpp"

namespace pulse::spsc
{

// SPSC Queue Consumer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_consumer final
{
private:
    friend class spsc_queue<T, Capacity>;
    using storage_type = spsc_queue_storage<T, Capacity>;
    using slot_type = typename storage_type::spsc_queue_slot;

    explicit spsc_queue_consumer(storage_type & storage) noexcept
        : consumer_cache_line_(&storage.consumer_cache_line())
        , producer_cache_line_(&storage.producer_cache_line())
        , slots_(&storage.slot(0))
        , local_read_seq_(consumer_cache_line_->seq.load(std::memory_order_relaxed))
        , cached_write_seq_(producer_cache_line_->seq.load(std::memory_order_acquire))
    {}

public:
    spsc_queue_consumer(const spsc_queue_consumer &) = delete;
    auto operator=(const spsc_queue_consumer &) -> spsc_queue_consumer & = delete;
    spsc_queue_consumer(spsc_queue_consumer &&) = delete;
    auto operator=(spsc_queue_consumer &&) -> spsc_queue_consumer & = delete;

public:
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

private:
    // Release the local read sequence to the consumer's cache line.
    auto _release() noexcept -> void
    {
        ++local_read_seq_;
        consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);
    }

    template <typename Consumer>
    [[nodiscard]]
    auto _try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>)
        -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        // The consumer owns its local read sequence; only refresh the peer's sequence when empty.
        const auto read_seq = local_read_seq_;
        auto & cached_write_seq = cached_write_seq_;

        // Check if the queue is empty.
        if (read_seq == cached_write_seq) {
            // Slow path. Read and update the consumer's cached write sequence.
            cached_write_seq = producer_cache_line_->seq.load(std::memory_order_acquire);

            // Check if the queue is still empty.
            if (read_seq == cached_write_seq) {
                return false;
            }
        }

        // Fast path.
        // Consume the element, then destroy it in the slot.
        auto * slot = slots_[read_seq & (Capacity - 1)].element();
        std::invoke(std::forward<Consumer>(consume), *slot);
        std::destroy_at(slot);

        // Release the slot only after consumption and destruction succeed.
        _release();

        return true;
    }

private:
    spsc_queue_cache_line * consumer_cache_line_;
    spsc_queue_cache_line * producer_cache_line_;
    slot_type * slots_;
    std::uint64_t local_read_seq_;
    std::uint64_t cached_write_seq_;
};

} // namespace pulse::spsc
