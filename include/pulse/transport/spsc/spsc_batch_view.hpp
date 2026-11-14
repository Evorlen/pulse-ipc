// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cassert>
#include <cstddef>
#include <type_traits>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/storage/slot.hpp"

namespace pulse::transport::spsc
{

// A non-owning view of raw slots or live elements. A ring wrap produces two physical segments.
// The underlying array contains slots, not T objects forming a T array; index through the slots.
// Views and their segments must not outlive their endpoint or the queue state owner.
template <concepts::QueueElementType T, bool Constructed>
class basic_spsc_queue_batch_view final
{
public:
    using reference = std::conditional_t<Constructed, T &, T *>;

    // A segment of the batch view.
    class segment_type final
    {
    private:
        friend class basic_spsc_queue_batch_view;

    public:
        segment_type() noexcept = default;

    private:
        explicit segment_type(storage::spsc_queue_slot<T> * slots, std::size_t size) noexcept
            : slots_(slots)
            , size_(size)
        {}

    public:
        // Return the number of slots in the segment.
        [[nodiscard]]
        auto size() const noexcept -> std::size_t
        {
            return size_;
        }

        // Return true if the segment is empty.
        [[nodiscard]]
        auto empty() const noexcept -> bool
        {
            return size_ == 0;
        }

        // Raw views return an address for construct_at; live views return an existing element. Caller must check index < size().
        [[nodiscard]]
        auto operator[](std::size_t index) const noexcept -> reference
        {
            assert(index < size());

            if constexpr (Constructed) {
                return slots_[index].element();
            } else {
                return slots_[index].address();
            }
        }

    private:
        storage::spsc_queue_slot<T> * slots_ {nullptr};
        std::size_t size_ {0};
    };

private:
    template <concepts::QueueElementType U, std::size_t Capacity>
        requires concepts::QueueCapacityType<Capacity>
    friend class spsc_queue_producer;

    template <concepts::QueueElementType U, std::size_t Capacity>
        requires concepts::QueueCapacityType<Capacity>
    friend class spsc_queue_consumer;

private:
    basic_spsc_queue_batch_view() noexcept = default;

    explicit basic_spsc_queue_batch_view(
        storage::spsc_queue_slot<T> * first,
        std::size_t first_size,
        storage::spsc_queue_slot<T> * second,
        std::size_t second_size
    ) noexcept
        : first_(first, first_size)
        , second_(second, second_size)
    {}

public:
    // Return the number of slots in the batch view.
    [[nodiscard]]
    auto size() const noexcept -> std::size_t
    {
        return first_.size() + second_.size();
    }

    // Return true if the batch view is empty.
    [[nodiscard]]
    auto empty() const noexcept -> bool
    {
        return size() == 0;
    }

    // Return the first segment of the batch view.
    [[nodiscard]]
    auto first() const noexcept -> segment_type
    {
        return first_;
    }

    // Return the second segment of the batch view.
    [[nodiscard]]
    auto second() const noexcept -> segment_type
    {
        return second_;
    }

    // Return the element at the given index. Caller must check index < size().
    [[nodiscard]]
    auto operator[](std::size_t index) const noexcept -> reference
    {
        assert(index < size());

        return index < first_.size() ? first_[index] : second_[index - first_.size()];
    }

private:
    segment_type first_;
    segment_type second_;
};

template <concepts::QueueElementType T>
using spsc_queue_write_view = basic_spsc_queue_batch_view<T, false>;

template <concepts::QueueElementType T>
using spsc_queue_read_view = basic_spsc_queue_batch_view<T, true>;

} // namespace pulse::transport::spsc
