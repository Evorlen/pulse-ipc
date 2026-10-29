// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <utility>

#include "pulse/common/result.hpp"
#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/spsc/spsc_consumer.hpp"
#include "pulse/spsc/spsc_producer.hpp"
#include "pulse/spsc/spsc_storage.hpp"

namespace pulse::spsc
{

// SPSC Queue.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue final
{
private:
    using storage_type = spsc_queue_storage<T, Capacity>;

public:
    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

    spsc_queue(spsc_queue &&) = delete;

    auto operator=(spsc_queue &&) -> spsc_queue & = delete;

    ~spsc_queue() noexcept = default;

public:
    // The private key restricts this constructor to validated factory results.
    explicit spsc_queue(
        typename storage_type::construction_key key,
        typename storage_type::region * region,
        bool owns_memory,
        bool owns_state
    ) noexcept
        : storage_(key, region, owns_memory, owns_state)
    {}

public:
    // Allocate one aligned region and initialize an empty queue.
    [[nodiscard]]
    static auto create(std::uint64_t schema_id = 0) noexcept -> common::result<spsc_queue>
    {
        auto region_memory = storage_type::_allocate(schema_id);
        if (!region_memory) {
            return std::unexpected(region_memory.error());
        }

        return common::result<spsc_queue>(
            std::in_place,
            typename storage_type::construction_key {},
            *region_memory,
            true,
            true
        );
    }

    // The memory, lifetime and synchronization requirements are the same as storage_type::create.
    [[nodiscard]]
    static auto create(
        std::span<std::byte> memory,
        spsc_queue_storage_mode mode,
        std::uint64_t schema_id = 0
    ) noexcept -> common::result<spsc_queue>
    {
        auto region_memory = storage_type::_bind(memory, mode, schema_id);
        if (!region_memory) {
            return std::unexpected(region_memory.error());
        }

        return common::result<spsc_queue>(
            std::in_place,
            typename storage_type::construction_key {},
            *region_memory,
            false,
            mode == spsc_queue_storage_mode::initialize
        );
    }

    // Only one producer may be active for a region, including through other attached queues.
    // Retire the previous producer and synchronize the handoff before attaching a replacement.
    // Endpoints must not outlive the queue state owner. Attachment does not claim a shared lock.
    [[nodiscard]]
    auto attach_producer() noexcept -> spsc_queue_producer<T, Capacity>
    {
        return spsc_queue_producer<T, Capacity> {storage_};
    }

    // Only one consumer may be active for a region; the same lifetime and handoff rules apply.
    [[nodiscard]]
    auto attach_consumer() noexcept -> spsc_queue_consumer<T, Capacity>
    {
        return spsc_queue_consumer<T, Capacity> {storage_};
    }

    // Return an approximate observation of whether the queue is full.
    // Concurrent operations may change the state before this call returns.
    [[nodiscard]]
    auto full() const noexcept -> bool
    {
        const auto write_seq = storage_.producer_cache_line().seq.load(std::memory_order_acquire);
        const auto read_seq = storage_.consumer_cache_line().seq.load(std::memory_order_acquire);
        return write_seq - read_seq == Capacity;
    }

    // Return an approximate observation of whether the queue is empty.
    // Concurrent operations may change the state before this call returns.
    [[nodiscard]]
    auto empty() const noexcept -> bool
    {
        const auto read_seq = storage_.consumer_cache_line().seq.load(std::memory_order_acquire);
        const auto write_seq = storage_.producer_cache_line().seq.load(std::memory_order_acquire);
        return read_seq == write_seq;
    }

    // Return the queue's capacity.
    [[nodiscard]]
    static constexpr auto capacity() noexcept -> std::size_t
    {
        return Capacity;
    }

    [[nodiscard]]
    static constexpr auto required_bytes() noexcept -> std::size_t
    {
        return spsc_queue_storage<T, Capacity>::required_bytes();
    }

    [[nodiscard]]
    static constexpr auto required_alignment() noexcept -> std::size_t
    {
        return spsc_queue_storage<T, Capacity>::required_alignment();
    }

private:
    storage_type storage_;
};

} // namespace pulse::spsc
