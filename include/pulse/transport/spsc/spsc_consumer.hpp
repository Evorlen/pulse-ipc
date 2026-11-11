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
#include "pulse/transport/spsc/spsc_batch_view.hpp"
#include "pulse/transport/spsc/spsc_storage.hpp"

namespace pulse::transport::spsc
{

// SPSC Queue Consumer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class alignas(config::cache_line_alignment) spsc_queue_consumer final
{
private:
    friend class spsc_queue<T, Capacity>;

    static constexpr std::size_t physical_capacity = std::bit_ceil(Capacity);
    static constexpr std::size_t index_mask = physical_capacity - 1;

public:
    spsc_queue_consumer(const spsc_queue_consumer &) = delete;

    auto operator=(const spsc_queue_consumer &) -> spsc_queue_consumer & = delete;

    spsc_queue_consumer(spsc_queue_consumer &&) = delete;

    auto operator=(spsc_queue_consumer &&) -> spsc_queue_consumer & = delete;

private:
    explicit spsc_queue_consumer(spsc_queue_storage<T, Capacity> & storage) noexcept
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
    // If assignment throws, the element stays queued; both it and the output may have changed.
    [[nodiscard]]
    auto try_pop_move(T & value) noexcept(std::is_nothrow_move_assignable_v<T>) -> bool
        requires std::is_move_assignable_v<T>
    {
        return _try_consume([&value](T & element) noexcept(std::is_nothrow_move_assignable_v<T>) -> void {
            value = std::move(element);
        });
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
        assert(borrowed_count_ == 0);

        const auto count = values.size();
        if (count == 0) [[unlikely]] {
            return true;
        }
        if (count > Capacity) [[unlikely]] {
            return false;
        }
        if (_read_available(count) < count) {
            return false;
        }

        _pop_batch<false>(values);
        return true;
    }

    // Move-assign the entire output, or return false without moving anything when data is insufficient.
    // An empty output succeeds. Destroy and release this batch only after all assignments succeed.
    // If assignment throws, propagate it without destroying elements or advancing the read sequence.
    // All elements remain queued, but attempted source and output elements, including the throwing
    // pair, may have changed and are not restored. Retrying does not recover the original values.
    [[nodiscard]]
    auto try_pop_bulk_move(std::span<T> values) noexcept(std::is_nothrow_move_assignable_v<T>) -> bool
        requires std::is_move_assignable_v<T>
    {
        assert(borrowed_count_ == 0);

        const auto count = values.size();
        if (count == 0) [[unlikely]] {
            return true;
        }
        if (count > Capacity) [[unlikely]] {
            return false;
        }
        if (_read_available(count) < count) {
            return false;
        }

        _pop_batch<true>(values);
        return true;
    }

    // Copy-assign the available prefix and return its size; leave the output suffix alone.
    // Empty output or an empty queue returns zero. Assignment failure leaves the whole prefix queued,
    // although output elements may have been modified. Release the prefix once on success.
    [[nodiscard]]
    auto try_pop_burst(std::span<T> values) noexcept(std::is_nothrow_copy_assignable_v<T>) -> std::size_t
        requires std::is_copy_assignable_v<T>
    {
        assert(borrowed_count_ == 0);

        if (values.empty()) [[unlikely]] {
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
    // If assignment throws, the whole prefix remains queued and the exception propagates.
    // Attempted source and output elements may have changed and are not restored, as in bulk move.
    [[nodiscard]]
    auto try_pop_burst_move(std::span<T> values) noexcept(std::is_nothrow_move_assignable_v<T>) -> std::size_t
        requires std::is_move_assignable_v<T>
    {
        assert(borrowed_count_ == 0);

        if (values.empty()) [[unlikely]] {
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
    auto try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>) -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        return _try_consume(std::forward<Consumer>(consume));
    }

    // Borrow the committed front element, or return nullptr when empty.
    // Repeated calls return the same element until release(). Do not destroy it yourself.
    // The pointer expires at release; endpoint operations must not be reentered.
    [[nodiscard]]
    auto try_borrow() noexcept -> T *
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

#ifndef NDEBUG
        borrowed_count_ = std::max(borrowed_count_, std::size_t {1});
#endif

        return std::addressof(slots_[read_seq & index_mask].element());
    }

    // Borrow up to max_count committed elements, split into two segments when the ring wraps.
    // Repeated acquisitions share the same front and stay valid until release, including release(0).
    // Do not destroy borrowed elements yourself. A zero request returns an empty view.
    [[nodiscard]]
    auto try_borrow_up_to(std::size_t max_count) noexcept -> spsc_queue_read_view<T>
    {
        if (max_count == 0) [[unlikely]] {
            return {};
        }

        const auto count = std::min(max_count, _read_available(max_count));
        if (count == 0) {
            return {};
        }

#ifndef NDEBUG
        borrowed_count_ = std::max(borrowed_count_, count);
#endif

        const auto index = static_cast<std::size_t>(local_read_seq_ & index_mask);
        const auto first_count = std::min(count, physical_capacity - index);
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
        assert(count <= borrowed_count_);

        const auto read_seq = local_read_seq_;

        _destroy_batch(read_seq, count);

#ifndef NDEBUG
        borrowed_count_ = 0;
#endif

        if (count != 0) {
            local_read_seq_ = read_seq + count;
            consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);
        }
    }

private:
    // Shared path for try_pop, try_pop_move, and try_consume. Invoke the consumer on the front
    // element, then destroy it and release the slot. Return false when empty without invoking.
    // Requires no outstanding borrow. If the consumer throws, the element stays queued and may
    // have been modified; the read sequence stays unchanged.
    template <typename Consumer>
    [[nodiscard]]
    auto _try_consume(Consumer && consume) noexcept(std::is_nothrow_invocable_v<Consumer, T &>) -> bool
        requires std::is_invocable_v<Consumer, T &>
    {
        assert(borrowed_count_ == 0);

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

        // Invoke the consumer on the element corresponding to the read sequence.
        auto & slot = slots_[read_seq & index_mask];
        std::invoke(std::forward<Consumer>(consume), slot.element());

        slot.destroy();

        // Release only after consumption and destruction succeed.
        local_read_seq_ = read_seq + 1;
        consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);

        return true;
    }

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
    auto _pop_batch(
        std::span<T> values
    ) noexcept(std::is_nothrow_assignable_v<T &, std::conditional_t<Move, T &&, const T &>>) -> void
    {
        const auto read_seq = local_read_seq_;
        const auto index = static_cast<std::size_t>(read_seq & index_mask);
        const auto first_count = std::min(values.size(), physical_capacity - index);
        const auto assign_segment =
            [](spsc_queue_slot<T> * slots, std::span<T> output) noexcept(
                std::is_nothrow_assignable_v<T &, std::conditional_t<Move, T &&, const T &>>
            ) -> void {
            for (std::size_t offset = 0; offset < output.size(); ++offset) {
                if constexpr (Move) {
                    output[offset] = std::move(slots[offset].element());
                } else {
                    output[offset] = std::as_const(slots[offset].element());
                }
            }
        };

        // Split at the ring boundary once; both segments still belong to a single batch.
        assign_segment(slots_ + index, values.first(first_count));
        assign_segment(slots_, values.subspan(first_count));

        // Keep every queued object alive if assignment throws. Outputs and moved-from queue
        // elements may already have changed; values and assignment side effects are not restored.
        // Destroy and release queue elements only after all assignments succeed.
        _destroy_batch(read_seq, values.size());

        local_read_seq_ = read_seq + values.size();
        consumer_cache_line_->seq.store(local_read_seq_, std::memory_order_release);
    }

    // Destroy a prefix in FIFO order without computing a wrapped index for each element.
    auto _destroy_batch(std::uint64_t read_seq, std::size_t count) noexcept -> void
    {
        const auto index = static_cast<std::size_t>(read_seq & index_mask);
        const auto first_count = std::min(count, physical_capacity - index);
        auto * first_slots = slots_ + index;
        for (std::size_t offset = 0; offset < first_count; ++offset) {
            first_slots[offset].destroy();
        }
        for (std::size_t offset = 0; offset < count - first_count; ++offset) {
            slots_[offset].destroy();
        }
    }

private:
    spsc_queue_cache_line * consumer_cache_line_;
    spsc_queue_cache_line * producer_cache_line_;
    spsc_queue_slot<T> * slots_;
    std::uint64_t local_read_seq_;
    std::uint64_t cached_read_limit_;

#ifndef NDEBUG
    std::size_t borrowed_count_ {0};
#endif
};

} // namespace pulse::transport::spsc
