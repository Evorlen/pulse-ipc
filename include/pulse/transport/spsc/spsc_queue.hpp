// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <new>
#include <span>
#include <type_traits>
#include <utility>

#include "pulse/common/result.hpp"
#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/transport/spsc/spsc_consumer.hpp"
#include "pulse/transport/spsc/spsc_error.hpp"
#include "pulse/transport/spsc/spsc_producer.hpp"
#include "pulse/transport/spsc/spsc_storage.hpp"

static_assert(std::atomic<bool>::is_always_lock_free);

namespace pulse::channel::spsc
{

template <concepts::InterProcessElementType U, std::size_t OtherCapacity>
    requires concepts::QueueCapacityType<OtherCapacity>
class spsc_channel;

} // namespace pulse::channel::spsc

namespace pulse::transport::spsc
{

// Ownership of the spsc queue storage.
enum class spsc_queue_storage_ownership : std::uint8_t
{
    Heap_owned = 0,
    In_place_owned = 1,
    Borrowed = 2,
};

// SPSC queue.
template <concepts::IntraProcessElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue final
{
private:
    template <concepts::InterProcessElementType U, std::size_t OtherCapacity>
        requires concepts::QueueCapacityType<OtherCapacity>
    friend class pulse::channel::spsc::spsc_channel;

    class construction_key final
    {
    private:
        friend class spsc_queue;

    private:
        construction_key() noexcept = default;
    };

public:
    explicit spsc_queue(
        construction_key,
        spsc_queue_storage<T, Capacity> * storage,
        spsc_queue_storage_ownership ownership
    ) noexcept
        : storage_(storage)
        , ownership_(ownership)
    {}

    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

    spsc_queue(spsc_queue &&) = delete;

    auto operator=(spsc_queue &&) -> spsc_queue & = delete;

    // All endpoints and access must end before the queue is destroyed.
    // Resolve producer reservations first; only published elements are cleaned up here.
    ~spsc_queue() noexcept
    {
        if (ownership_ == spsc_queue_storage_ownership::Borrowed) {
            return;
        }

        if constexpr (!std::is_trivially_destructible_v<T>) {
            constexpr auto index_mask = std::bit_ceil(Capacity) - 1;

            // Destroy elements from read sequence to write sequence.
            auto read_seq = storage_->consumer_cache_line().seq.load(std::memory_order_relaxed);
            const auto write_seq = storage_->producer_cache_line().seq.load(std::memory_order_relaxed);

            while (read_seq != write_seq) {
                storage_->slot(read_seq & index_mask).destroy();
                ++read_seq;
            }
        }

        if (ownership_ == spsc_queue_storage_ownership::Heap_owned) {
            delete storage_;
        } else {
            // In-place owned.
            std::destroy_at(storage_);
        }
    }

private:
    // A channel validates storage and coordinates the shared state's lifetime.
    explicit spsc_queue(spsc_queue_storage<T, Capacity> * storage) noexcept
        : spsc_queue(construction_key {}, storage, spsc_queue_storage_ownership::Borrowed)
    {}

public:
    // Allocate aligned storage and initialize an empty queue.
    [[nodiscard]]
    static auto create() -> common::result<spsc_queue>
    {
        auto * storage = new spsc_queue_storage<T, Capacity>;

        return common::result<spsc_queue>(
            std::in_place,
            construction_key {},
            storage,
            spsc_queue_storage_ownership::Heap_owned
        );
    }

    // Initialize an empty queue in unused, writable storage with exclusive access.
    // The caller retains the allocation and must keep it alive until the queue is destroyed.
    // Existing queue storage must not be passed here: this always constructs a new state.
    [[nodiscard]]
    static auto create(std::span<std::byte> memory) noexcept -> common::result<spsc_queue>
    {
        auto storage = _initialize_storage(memory);
        if (!storage) [[unlikely]] {
            return std::unexpected(storage.error());
        }

        return common::result<spsc_queue>(
            std::in_place,
            construction_key {},
            *storage,
            spsc_queue_storage_ownership::In_place_owned
        );
    }

    // Return the required size of spsc_queue storage.
    [[nodiscard]]
    static constexpr auto required_bytes() noexcept -> std::size_t
    {
        return sizeof(spsc_queue_storage<T, Capacity>);
    }

    // Return the required alignment of spsc_queue storage.
    [[nodiscard]]
    static constexpr auto required_alignment() noexcept -> std::size_t
    {
        return alignof(spsc_queue_storage<T, Capacity>);
    }

    // Each role may have only one open producer. Closing or destroying it allows reattachment.
    // Reattachment preserves queued elements and sequence numbers.
    // Concurrent attachment is allowed; producer borrows the queue.
    // Return ProducerAlreadyAttached if the role is occupied.
    [[nodiscard]]
    auto try_attach_producer() noexcept -> common::result<spsc_queue_producer<T, Capacity>>
    {
        return _try_attach_producer(&producer_attached_);
    }

    // Each role may have only one open consumer. Closing or destroying it allows reattachment.
    // Reattachment preserves queued elements and sequence numbers.
    // Concurrent attachment is allowed; consumer borrows the queue.
    // Return ConsumerAlreadyAttached if the role is occupied.
    [[nodiscard]]
    auto try_attach_consumer() noexcept -> common::result<spsc_queue_consumer<T, Capacity>>
    {
        return _try_attach_consumer(&consumer_attached_);
    }

private:
    // Initialize unused, writable storage with exclusive access, without creating a queue owner.
    // The caller coordinates the new state's lifetime independently of the allocation.
    [[nodiscard]]
    static auto _initialize_storage(std::span<std::byte> memory) noexcept
        -> common::result<spsc_queue_storage<T, Capacity> *>
    {
        if (memory.data() == nullptr) [[unlikely]] {
            return std::unexpected(common::error {spsc_error_code::InvalidStorage});
        }
        if (memory.size() < required_bytes()) [[unlikely]] {
            return std::unexpected(common::error {spsc_error_code::StorageTooSmall});
        }
        if (reinterpret_cast<std::uintptr_t>(memory.data()) % required_alignment() != 0) [[unlikely]] {
            return std::unexpected(common::error {spsc_error_code::StorageNotAligned});
        }

        return ::new (static_cast<void *>(memory.data())) spsc_queue_storage<T, Capacity>;
    }

    // Claim the producer role using a local or shared attachment flag.
    [[nodiscard]]
    auto _try_attach_producer(std::atomic<bool> * attached) noexcept
        -> common::result<spsc_queue_producer<T, Capacity>>
    {
        bool expected = false;
        const auto success = attached->compare_exchange_strong(
            expected,
            true,
            std::memory_order_acquire,
            std::memory_order_relaxed
        );
        if (!success) {
            return std::unexpected(common::error {spsc_error_code::ProducerAlreadyAttached});
        }

        return common::result<spsc_queue_producer<T, Capacity>>(
            std::in_place,
            typename spsc_queue_producer<T, Capacity>::construction_key {},
            *storage_,
            attached
        );
    }

    // Claim the consumer role using a local or shared attachment flag.
    [[nodiscard]]
    auto _try_attach_consumer(std::atomic<bool> * attached) noexcept
        -> common::result<spsc_queue_consumer<T, Capacity>>
    {
        bool expected = false;
        const auto success = attached->compare_exchange_strong(
            expected,
            true,
            std::memory_order_acquire,
            std::memory_order_relaxed
        );
        if (!success) {
            return std::unexpected(common::error {spsc_error_code::ConsumerAlreadyAttached});
        }

        return common::result<spsc_queue_consumer<T, Capacity>>(
            std::in_place,
            typename spsc_queue_consumer<T, Capacity>::construction_key {},
            *storage_,
            attached
        );
    }

private:
    spsc_queue_storage<T, Capacity> * storage_;
    std::atomic<bool> producer_attached_ {false};
    std::atomic<bool> consumer_attached_ {false};
    spsc_queue_storage_ownership ownership_;
};

} // namespace pulse::transport::spsc
