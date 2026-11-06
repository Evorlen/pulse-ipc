// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/config/config.hpp"
#include "pulse/spsc/spsc_batch_view.hpp"
#include "pulse/spsc/spsc_storage.hpp"

namespace pulse::spsc
{

// SPSC Queue Consumer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class alignas(config::cache_line_alignment) spsc_queue_consumer final
{
private:
    friend class spsc_queue<T, Capacity>;
    using storage_type = spsc_queue_storage<T, Capacity>;
    using slot_type = spsc_queue_slot<T>;

public:
    spsc_queue_consumer(const spsc_queue_consumer &) = delete;

    auto operator=(const spsc_queue_consumer &) -> spsc_queue_consumer & = delete;

    spsc_queue_consumer(spsc_queue_consumer &&) = delete;

    auto operator=(spsc_queue_consumer &&) -> spsc_queue_consumer & = delete;

private:
    explicit spsc_queue_consumer(storage_type & storage) noexcept
        : consumer_cache_line_(&storage.consumer_cache_line())
        , producer_cache_line_(&storage.producer_cache_line())
        , slots_(&storage.slot(0))
        , local_read_seq_(consumer_cache_line_->seq.load(std::memory_order_relaxed))
        , cached_read_limit_(producer_cache_line_->seq.load(std::memory_order_acquire))
    {}

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

    // Copy-assign the entire output, or return false without assigning anything when data is insufficient.
    // An empty output succeeds. Destroy and release this batch only after all assignments succeed.
    // If assignment throws, all elements remain queued, but output elements may have been modified.
    // For all batch operations, output must contain live objects, remain valid throughout the call,
    // and not overlap queue storage. Do not reenter this endpoint or mix consumption with a borrow.
    [[nodiscard]]
    auto try_pop_bulk(std::span<T> values) noexcept(std::is_nothrow_copy_assignable_v<T>) -> bool
        requires std::is_copy_assignable_v<T>
    {
        assert(peeked_count_ == 0);
        const auto count = values.size();
        if (count == 0) {
            return true;
        }
        if (count > Capacity || _read_available(count) < count) {
            return false;
        }

        _pop_batch<false>(values);
        return true;
    }

    // Move-assign the entire output, or return false without moving anything when data is insufficient.
    // An empty output succeeds. Only non-throwing assignment from T&& is supported.
    [[nodiscard]]
    auto try_pop_bulk_move(std::span<T> values) noexcept -> bool
        requires std::is_nothrow_move_assignable_v<T>
    {
        assert(peeked_count_ == 0);
        const auto count = values.size();
        if (count == 0) {
            return true;
        }
        if (count > Capacity || _read_available(count) < count) {
            return false;
        }

        _pop_batch<true>(values);
        return true;
    }

    // Copy-assign the available prefix and return its size; leave the output suffix alone.
    // Empty output or an empty queue returns zero. Assignment failure leaves the whole prefix queued,
    // although output elements may have been modified. Release the prefix once on success.
    [[nodiscard]]
    auto try_pop_burst(std::span<T> values) noexcept(std::is_nothrow_copy_assignable_v<T>)
        -> std::size_t
        requires std::is_copy_assignable_v<T>
    {
        assert(peeked_count_ == 0);
        if (values.empty()) {
            return 0;
        }

        const auto count = std::min(values.size(), _read_available(values.size()));
        if (count != 0) {
            _pop_batch<false>(values.first(count));
        }
        return count;
    }

    // Move-assign the available prefix and return its size; leave the output suffix alone.
    // Empty output or an empty queue returns zero without moving anything. Release once on success.
    [[nodiscard]]
    auto try_pop_burst_move(std::span<T> values) noexcept -> std::size_t
        requires std::is_nothrow_move_assignable_v<T>
    {
        assert(peeked_count_ == 0);
        if (values.empty()) {
            return 0;
        }

        const auto count = std::min(values.size(), _read_available(values.size()));
        if (count != 0) {
            _pop_batch<true>(values.first(count));
        }
        return count;
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

    // Borrow the committed front element, or return nullptr when empty.
    // Repeated calls return the same element until release(). Do not destroy it yourself.
    // The pointer expires at release; endpoint operations must not be reentered.
    [[nodiscard]]
    auto try_peek() noexcept -> T *
    {
        // The consumer owns its local read sequence; only refresh the peer's sequence when empty.
        const auto read_seq = local_read_seq_;

        // Check if the queue is empty.
        if (read_seq == cached_read_limit_) {
            // Slow path. Read and update the consumer's cached write sequence.
            cached_read_limit_ = producer_cache_line_->seq.load(std::memory_order_acquire);

            // Check if the queue is still empty.
            if (read_seq == cached_read_limit_) {
                return nullptr;
            }
        }

        peeked_count_ = std::max(peeked_count_, std::size_t {1});
        return slots_[read_seq & (Capacity - 1)].element();
    }

    // Borrow up to max_count committed elements, split into two segments when the ring wraps.
    // Repeated acquisitions share the same front and stay valid until release, including release(0).
    // Do not destroy borrowed elements yourself. A zero request returns an empty view.
    [[nodiscard]]
    auto try_peek(std::size_t max_count) noexcept -> spsc_queue_read_view<T>
    {
        if (max_count == 0) {
            return {};
        }
        const auto count = std::min(max_count, _read_available(max_count));
        if (count == 0) {
            return {};
        }

        peeked_count_ = std::max(peeked_count_, count);
        const auto index = static_cast<std::size_t>(local_read_seq_ & (Capacity - 1));
        const auto first_count = std::min(count, Capacity - index);
        return spsc_queue_read_view<T> {slots_ + index, first_count, slots_, count - first_count};
    }

    // Destroy the borrowed element before allowing the producer to reuse its slot.
    // Requires a successful peek. Call exactly once; do not mix ordinary consumption
    // with an outstanding borrow. Retiring an endpoint leaves peeks queued.
    auto release() noexcept -> void
    {
        release(1);
    }

    // Destroy and release a borrowed prefix once; invalidate every outstanding pointer/view.
    // count must fit a successful acquisition. The suffix stays queued and must be borrowed again.
    // release(0) only ends the borrow, preserving all elements and publishing no read sequence.
    auto release(std::size_t count) noexcept -> void
    {
        assert(count <= peeked_count_);
        const auto read_seq = local_read_seq_;

        if constexpr (!std::is_trivially_destructible_v<T>) {
            for (std::size_t index = 0; index < count; ++index) {
                std::destroy_at(slots_[(read_seq + index) & (Capacity - 1)].element());
            }
        }

        peeked_count_ = 0;
        if (count != 0) {
            local_read_seq_ = read_seq + count;
            consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);
        }
    }

private:
    // Cached data is conservative. Refresh when the requested batch does not fit in the cache,
    // even when some cached elements remain. Unsigned subtraction handles sequence wrap.
    [[nodiscard]]
    auto _read_available(std::size_t requested) noexcept -> std::size_t
    {
        auto available = static_cast<std::size_t>(cached_read_limit_ - local_read_seq_);
        if (available < std::min(requested, Capacity)) {
            cached_read_limit_ = producer_cache_line_->seq.load(std::memory_order_acquire);
            available = static_cast<std::size_t>(cached_read_limit_ - local_read_seq_);
        }
        return available;
    }

    // The caller has checked that this nonempty batch is available.
    template <bool Move>
    auto _pop_batch(std::span<T> values) noexcept(Move || std::is_nothrow_copy_assignable_v<T>)
        -> void
    {
        const auto read_seq = local_read_seq_;
        for (std::size_t index = 0; index < values.size(); ++index) {
            auto * slot = slots_[(read_seq + index) & (Capacity - 1)].element();
            if constexpr (Move) {
                values[index] = std::move(*slot);
            } else {
                values[index] = std::as_const(*slot);
            }
        }

        if constexpr (!std::is_trivially_destructible_v<T>) {
            // Keep every queued object alive if a copy assignment throws. Outputs belong to the caller
            // and may already have changed; neither their values nor arbitrary assignment side effects
            // can be rolled back here. Queue elements are destroyed only after all assignments succeed.
            for (std::size_t index = 0; index < values.size(); ++index) {
                std::destroy_at(slots_[(read_seq + index) & (Capacity - 1)].element());
            }
        }

        local_read_seq_ = read_seq + values.size();
        consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);
    }

    template <typename Consumer>
    [[nodiscard]]
    auto _try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>)
        -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        assert(peeked_count_ == 0);
        // The consumer owns its local read sequence; only refresh the peer's sequence when empty.
        const auto read_seq = local_read_seq_;

        // Check if the queue is empty.
        if (read_seq == cached_read_limit_) {
            // Slow path. Read and update the consumer's cached write sequence.
            cached_read_limit_ = producer_cache_line_->seq.load(std::memory_order_acquire);

            // Check if the queue is still empty.
            if (read_seq == cached_read_limit_) {
                return false;
            }
        }

        auto * slot = slots_[read_seq & (Capacity - 1)].element();
        std::invoke(std::forward<Consumer>(consume), *slot);

        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::destroy_at(slot);
        }

        // Release only after consumption and destruction succeed.
        local_read_seq_ = read_seq + 1;
        consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);

        return true;
    }

private:
    spsc_queue_cache_line * consumer_cache_line_;
    spsc_queue_cache_line * producer_cache_line_;
    slot_type * slots_;
    std::uint64_t local_read_seq_;
    std::uint64_t cached_read_limit_;
    std::size_t peeked_count_ {0};
};

} // namespace pulse::spsc
