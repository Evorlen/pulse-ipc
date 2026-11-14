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
#include <optional>
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

namespace pulse::transport::spsc
{

// SPSC Queue.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue final
{
private:
    using storage_type = spsc_queue_storage<T, Capacity>;

    class construction_key final
    {
    private:
        friend class spsc_queue;

    private:
        construction_key() noexcept = default;
    };

public:
    explicit spsc_queue(construction_key, storage_type * storage, bool own_memory) noexcept
        : storage_(storage)
        , own_memory_(own_memory)
    {}

    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

    spsc_queue(spsc_queue &&) = delete;

    auto operator=(spsc_queue &&) -> spsc_queue & = delete;

    // All endpoints and access must end before the queue is destroyed.
    // Resolve producer reservations first; only published elements are cleaned up here.
    ~spsc_queue() noexcept
    {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            constexpr auto index_mask = std::bit_ceil(Capacity) - 1;
            auto read_seq = storage_->consumer_cache_line().seq.load(std::memory_order_relaxed);
            const auto write_seq = storage_->producer_cache_line().seq.load(std::memory_order_relaxed);

            while (read_seq != write_seq) {
                storage_->slot(read_seq & index_mask).destroy();
                ++read_seq;
            }
        }

        if (own_memory_) {
            delete storage_;
        } else {
            std::destroy_at(storage_);
        }
    }

public:
    // Allocate aligned storage and initialize an empty queue.
    [[nodiscard]]
    static auto create() noexcept -> common::result<spsc_queue>
    {
        auto * storage = new (std::nothrow) storage_type;
        if (storage == nullptr) {
            return std::unexpected(common::error {spsc_error_code::StorageAllocationFailed});
        }

        return common::result<spsc_queue>(std::in_place, construction_key {}, storage, true);
    }

    // Initialize an empty queue in unused, writable storage with exclusive access.
    // The caller retains the allocation and must keep it alive until the queue is destroyed.
    // Existing queue storage must not be passed here: this always constructs a new state.
    [[nodiscard]]
    static auto create(std::span<std::byte> memory) noexcept -> common::result<spsc_queue>
    {
        if (memory.data() == nullptr) {
            return std::unexpected(common::error {spsc_error_code::StorageNull});
        }
        if (memory.size() < required_bytes()) {
            return std::unexpected(common::error {spsc_error_code::StorageTooSmall});
        }
        if (reinterpret_cast<std::uintptr_t>(memory.data()) % required_alignment() != 0) {
            return std::unexpected(common::error {spsc_error_code::StorageNotAligned});
        }

        auto * storage = ::new (static_cast<void *>(memory.data())) storage_type;
        return common::result<spsc_queue>(std::in_place, construction_key {}, storage, false);
    }

    // Return the required size of spsc_queue storage.
    [[nodiscard]]
    static constexpr auto required_bytes() noexcept -> std::size_t
    {
        return sizeof(storage_type);
    }

    // Return the required alignment of spsc_queue storage.
    [[nodiscard]]
    static constexpr auto required_alignment() noexcept -> std::size_t
    {
        return alignof(storage_type);
    }

    // Each role may be attached only once during this queue's lifetime, including after
    // its endpoint is destroyed. Concurrent attachment is allowed; endpoints borrow the queue.
    [[nodiscard]]
    auto try_attach_producer() noexcept -> std::optional<spsc_queue_producer<T, Capacity>>
    {
        bool expected = false;
        const bool success = producer_attached_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_relaxed,
            std::memory_order_relaxed
        );

        if (!success) {
            return std::nullopt;
        }

        return std::optional<spsc_queue_producer<T, Capacity>>(
            std::in_place,
            typename spsc_queue_producer<T, Capacity>::construction_key {},
            *storage_
        );
    }

    // The same attachment and lifetime rules apply to the consumer.
    [[nodiscard]]
    auto try_attach_consumer() noexcept -> std::optional<spsc_queue_consumer<T, Capacity>>
    {
        bool expected = false;
        const bool success = consumer_attached_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_relaxed,
            std::memory_order_relaxed
        );

        if (!success) {
            return std::nullopt;
        }

        return std::optional<spsc_queue_consumer<T, Capacity>>(
            std::in_place,
            typename spsc_queue_consumer<T, Capacity>::construction_key {},
            *storage_
        );
    }

private:
    storage_type * storage_;
    std::atomic<bool> producer_attached_ {false};
    std::atomic<bool> consumer_attached_ {false};
    bool own_memory_;
};

} // namespace pulse::transport::spsc
