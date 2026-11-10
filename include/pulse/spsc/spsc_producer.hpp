// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
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

// SPSC Queue Producer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class alignas(config::cache_line_alignment) spsc_queue_producer final
{
private:
    friend class spsc_queue<T, Capacity>;

    static constexpr std::size_t physical_capacity = std::bit_ceil(Capacity);
    static constexpr std::size_t index_mask = physical_capacity - 1;

public:
    spsc_queue_producer(const spsc_queue_producer &) = delete;

    auto operator=(const spsc_queue_producer &) -> spsc_queue_producer & = delete;

    spsc_queue_producer(spsc_queue_producer &&) = delete;

    auto operator=(spsc_queue_producer &&) -> spsc_queue_producer & = delete;

private:
    explicit spsc_queue_producer(spsc_queue_storage<T, Capacity> & storage) noexcept
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

    // Copy the entire input, or return false without constructing anything when space is insufficient.
    // An empty input succeeds. Publish once after all elements have been constructed.
    // If copying throws, destroy this batch's constructed elements and leave the queue unchanged.
    // For all batch operations, input must not overlap queue storage; do not reenter this endpoint
    // or mix pushes with an outstanding reservation. Input must remain valid throughout the call.
    [[nodiscard]]
    auto try_push_bulk(std::span<const T> values) noexcept(std::is_nothrow_copy_constructible_v<T>) -> bool
        requires std::is_copy_constructible_v<T>
    {
        assert(reserved_count_ == 0);

        const auto count = values.size();
        if (count == 0) [[unlikely]] {
            return true;
        }
        if (count > Capacity) [[unlikely]] {
            return false;
        }
        if (_write_available(count) < count) {
            return false;
        }

        _push_batch<false>(values);

        return true;
    }

    // Move the entire input, or return false without moving anything when space is insufficient.
    // An empty input succeeds. If moving throws, destroy this batch's constructed elements
    // and rethrow without publishing anything. Already attempted source elements, including
    // the one whose construction threw, may have changed and are not restored.
    // Source objects remain alive in their moved-from states; their container's size does not change.
    [[nodiscard]]
    auto try_push_bulk_move(std::span<T> values) noexcept(std::is_nothrow_move_constructible_v<T>) -> bool
        requires std::is_move_constructible_v<T>
    {
        assert(reserved_count_ == 0);

        const auto count = values.size();
        if (count == 0) [[unlikely]] {
            return true;
        }
        if (count > Capacity) [[unlikely]] {
            return false;
        }
        if (_write_available(count) < count) {
            return false;
        }

        _push_batch<true>(values);

        return true;
    }

    // Copy the available prefix and return its size. An empty input or full queue returns zero.
    // Publish the prefix once. If copying throws, roll back the entire prefix and rethrow.
    [[nodiscard]]
    auto try_push_burst(std::span<const T> values) noexcept(std::is_nothrow_copy_constructible_v<T>)
        -> std::size_t
        requires std::is_copy_constructible_v<T>
    {
        assert(reserved_count_ == 0);

        if (values.empty()) [[unlikely]] {
            return 0;
        }

        const auto count = std::min(values.size(), _write_available(values.size()));
        if (count != 0) {
            _push_batch<false>(values.first(count));
        }
        return count;
    }

    // Move only the available prefix and return its size; leave the remaining source elements alone.
    // Publish the prefix once. An empty input or full queue returns zero without moving anything.
    // If moving throws, destroy the constructed prefix and rethrow without publishing anything.
    // Already attempted source elements may have changed and are not restored, as in bulk move.
    [[nodiscard]]
    auto try_push_burst_move(std::span<T> values) noexcept(std::is_nothrow_move_constructible_v<T>)
        -> std::size_t
        requires std::is_move_constructible_v<T>
    {
        assert(reserved_count_ == 0);

        if (values.empty()) [[unlikely]] {
            return 0;
        }

        const auto count = std::min(values.size(), _write_available(values.size()));
        if (count != 0) {
            _push_batch<true>(values.first(count));
        }
        return count;
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
            cached_write_limit_ = consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity;

            // Check if the queue is still full.
            if (write_seq == cached_write_limit_) {
                return nullptr;
            }
        }

#ifndef NDEBUG
        reserved_count_ = std::max(reserved_count_, std::size_t {1});
#endif

        return slots_[write_seq & index_mask].address();
    }

    // Reserve up to max_count slots without constructing or publishing any T objects.
    // Repeated acquisitions share the same start; their views remain valid until commit/cancel.
    // Construct each committed element exactly once. A zero request returns an empty view.
    [[nodiscard]]
    auto try_reserve_up_to(std::size_t max_count) noexcept -> spsc_queue_write_view<T>
    {
        if (max_count == 0) [[unlikely]] {
            return {};
        }

        const auto count = std::min(max_count, _write_available(max_count));
        if (count == 0) {
            return {};
        }

#ifndef NDEBUG
        reserved_count_ = std::max(reserved_count_, count);
#endif

        const auto index = static_cast<std::size_t>(local_write_seq_ & index_mask);
        const auto first_count = std::min(count, physical_capacity - index);
        return spsc_queue_write_view<T> {slots_ + index, first_count, slots_, count - first_count};
    }

    // Requires a successful reservation containing a live T. Call exactly once.
    // The caller must not mix ordinary pushes with an outstanding reservation.
    auto commit() noexcept -> void
    {
        commit(1);
    }

    // Publish a constructed prefix once, then invalidate every outstanding reservation/view.
    // count must fit a successful acquisition; uncommitted slots must contain no live objects.
    // The caller must destroy any constructed suffix before committing a shorter prefix.
    // commit(0) ends the reservation without publishing. The endpoint never constructs/destroys here.
    auto commit(std::size_t count) noexcept -> void
    {
        assert(count <= reserved_count_);

#ifndef NDEBUG
        reserved_count_ = 0;
#endif

        if (count != 0) {
            local_write_seq_ += count;
            producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);
        }
    }

    // End all reservations without publishing; caller must destroy any constructed T first.
    // Raw storage has no known lifetime, so cancel never destroys elements itself.
    // Resolve reservations before retiring the endpoint; its destructor does not clean them up.
    auto cancel() noexcept -> void
    {
#ifndef NDEBUG
        reserved_count_ = 0;
#endif
    }

private:
    // Shared path for try_push and try_emplace. Construct one T from the forwarded arguments,
    // or return false when full without constructing anything. Requires no outstanding reservation.
    // If construction throws, the write sequence stays unchanged.
    template <typename... Args>
    auto _try_emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> bool
        requires std::is_constructible_v<T, Args...>
    {
        assert(reserved_count_ == 0);

        // The producer owns its local write sequence; only refresh the peer's sequence when full.
        const auto write_seq = local_write_seq_;

        // Check if the queue is full.
        if (write_seq == cached_write_limit_) {
            // Slow path. Read and update the producer's cached read sequence.
            cached_write_limit_ = consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity;

            // Check if the queue is still full.
            if (write_seq == cached_write_limit_) {
                return false;
            }
        }

        // Construct T in the slot corresponding to the write sequence.
        slots_[write_seq & index_mask].construct(std::forward<Args>(args)...);

        // Publish only after construction succeeds; exceptions leave the sequence unchanged.
        local_write_seq_ = write_seq + 1;
        producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);

        return true;
    }

    // Cached space is conservative. Refresh whenever it cannot satisfy the requested batch,
    // even if it still has room for a single element. Unsigned subtraction handles sequence wrap.
    [[nodiscard]]
    auto _write_available(std::size_t requested) noexcept -> std::size_t
    {
        auto available = static_cast<std::size_t>(cached_write_limit_ - local_write_seq_);
        // If the cached space is not enough, refresh the cached write limit, though it might still be not enough.
        if (available < std::min(requested, Capacity)) {
            cached_write_limit_ = consumer_cache_line_->seq.load(std::memory_order_acquire) + Capacity;
            available = static_cast<std::size_t>(cached_write_limit_ - local_write_seq_);
        }
        return available;
    }

    // The caller has checked that this nonempty batch fits in the available slots.
    template <bool Move>
    auto _push_batch(
        std::span<std::conditional_t<Move, T, const T>> values
    ) noexcept(std::is_nothrow_constructible_v<T, std::conditional_t<Move, T &&, const T &>>) -> void
    {
        constexpr bool is_nothrow_constructible =
            std::is_nothrow_constructible_v<T, std::conditional_t<Move, T &&, const T &>>;

        const auto write_seq = local_write_seq_;
        const auto index = static_cast<std::size_t>(write_seq & index_mask);
        const auto first_count = std::min(values.size(), physical_capacity - index);
        auto * first_slots = slots_ + index;
        std::size_t constructed = 0;
        const auto construct_one =
            [](spsc_queue_slot<T> & slot,
               std::conditional_t<Move, T, const T> & value) noexcept(is_nothrow_constructible) -> void {
            if constexpr (Move) {
                slot.construct(std::move(value));
            } else {
                slot.construct(value);
            }
        };
        const auto construct = [&]() noexcept(is_nothrow_constructible) -> void {
            // Split at the ring boundary once, keeping each loop's slot access contiguous.
            for (; constructed < first_count; ++constructed) {
                construct_one(first_slots[constructed], values[constructed]);
            }
            for (; constructed < values.size(); ++constructed) {
                construct_one(slots_[constructed - first_count], values[constructed]);
            }
        };

        if constexpr (is_nothrow_constructible) {
            construct();
        } else {
            // If construction throws, destroy the already constructed elements and rethrow.
            try {
                construct();
            } catch (...) {
                // Roll back the second segment before the first, in reverse construction order.
                while (constructed > first_count) {
                    --constructed;
                    slots_[constructed - first_count].destroy();
                }
                while (constructed != 0) {
                    --constructed;
                    first_slots[constructed].destroy();
                }

                throw;
            }
        }

        // Neither the local nor the shared write sequence advances until construction succeeds.
        local_write_seq_ = write_seq + values.size();
        producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);
    }

private:
    spsc_queue_cache_line * producer_cache_line_;
    spsc_queue_cache_line * consumer_cache_line_;
    spsc_queue_slot<T> * slots_;
    std::uint64_t local_write_seq_;
    std::uint64_t cached_write_limit_;

#ifndef NDEBUG
    std::size_t reserved_count_ {0};
#endif
};

} // namespace pulse::spsc
