// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/config/config.hpp"

namespace pulse::spsc
{

// Header for the shared queue.
// With 64-byte cache lines, the header occupies two cache lines.
struct alignas(config::cache_line_alignment) spsc_queue_header final
{
    std::uint64_t magic {0};
    std::uint16_t version_major {0};
    std::uint16_t version_minor {0};
    std::uint32_t header_size {0};
    std::uint64_t region_size {0};
    std::uint64_t capacity {0};
    std::uint32_t slot_size {0};
    std::uint32_t slot_alignment {0};
    std::uint32_t slot_stride {0};
    std::uint64_t slots_offset {0};
    std::uint64_t schema_id {0};
    std::uint32_t flags {0};
    std::atomic<std::uint32_t> state {0};
    std::array<std::byte, 24> reserved {};
};

// Cache line for the producer and consumer.
struct alignas(config::cache_line_alignment) spsc_queue_cache_line final
{
    std::atomic<std::uint64_t> seq {0};
};

static_assert(
    sizeof(spsc_queue_cache_line) <= config::cache_line_alignment,
    "the size of spsc_queue_cache_line must be less than or equal to cache_line_alignment."
);

// Initializing external memory owns the queue state's lifetime, but not the allocation.
// Attaching borrows an existing state and never initializes or destroys it.
enum class spsc_queue_storage_mode : std::uint8_t
{
    initialize = 0,
    attach = 1,
};

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue;

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_producer;

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_consumer;

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_storage final
{
private:
    friend class spsc_queue_producer<T, Capacity>;
    friend class spsc_queue_consumer<T, Capacity>;

    struct alignas(T) spsc_queue_slot final
    {
        std::array<std::byte, sizeof(T)> storage;

        [[nodiscard]]
        auto address() noexcept -> T *
        {
            return reinterpret_cast<T *>(storage.data());
        }

        // Only call when this slot contains a live T.
        [[nodiscard]]
        auto element() noexcept -> T *
        {
            return std::launder(address());
        }
    };

    static constexpr std::size_t slots_alignment = alignof(T) > config::cache_line_alignment
                                                       ? alignof(T)
                                                       : config::cache_line_alignment;

    // This is the entire shared region. It contains no process-local pointers or caches.
    struct region final
    {
        spsc_queue_header header;
        spsc_queue_cache_line producer_cache_line;
        spsc_queue_cache_line consumer_cache_line;
        alignas(slots_alignment) std::array<spsc_queue_slot, Capacity> slots;
    };

    static_assert(std::is_standard_layout_v<region>);
    static_assert(sizeof(spsc_queue_slot) <= std::numeric_limits<std::uint32_t>::max());
    static_assert(alignof(T) <= std::numeric_limits<std::uint32_t>::max());

    static constexpr std::uint64_t magic = 0x50554C5345535053;
    static constexpr std::uint16_t version_major = 1;
    static constexpr std::uint16_t version_minor = 0;
    static constexpr std::uint32_t ready_state = 1;

public:
    // Allocate one aligned region and initialize an empty queue.
    explicit spsc_queue_storage(std::uint64_t schema_id = 0)
        : region_(new region)
        , owns_memory_(true)
        , owns_state_(true)
    {
        _initialize_header(*region_, schema_id);
    }

    // Initialization requires unused, writable raw storage and exclusive access.
    // Attachment requires a live region initialized with the same T, Capacity and ABI.
    // Initialization must finish and be synchronized with the caller before attachment.
    // schema_id must match the initializer; zero provides no application-specific type identity.
    explicit spsc_queue_storage(
        std::span<std::byte> memory,
        spsc_queue_storage_mode mode,
        std::uint64_t schema_id = 0
    )
        : region_(_bind(memory, mode, schema_id))
        , owns_memory_(false)
        , owns_state_(mode == spsc_queue_storage_mode::initialize)
    {}

    spsc_queue_storage(const spsc_queue_storage &) = delete;
    auto operator=(const spsc_queue_storage &) -> spsc_queue_storage & = delete;
    spsc_queue_storage(spsc_queue_storage &&) = delete;
    auto operator=(spsc_queue_storage &&) -> spsc_queue_storage & = delete;

    // The state owner must outlive all attached views and endpoints.
    // All queue access must stop before the state owner is destroyed.
    ~spsc_queue_storage() noexcept
    {
        if (!owns_state_) {
            return;
        }

        auto read_seq = region_->consumer_cache_line.seq.load(std::memory_order_relaxed);
        const auto write_seq = region_->producer_cache_line.seq.load(std::memory_order_relaxed);
        while (read_seq != write_seq) {
            std::destroy_at(slot(read_seq & (Capacity - 1)).element());
            ++read_seq;
        }

        if (owns_memory_) {
            delete region_;
        } else {
            std::destroy_at(region_);
        }
    }

public:
    [[nodiscard]]
    static constexpr auto required_bytes() noexcept -> std::size_t
    {
        return sizeof(region);
    }

    [[nodiscard]]
    static constexpr auto required_alignment() noexcept -> std::size_t
    {
        return alignof(region);
    }

    [[nodiscard]]
    auto header() const noexcept -> const spsc_queue_header &
    {
        return region_->header;
    }

    [[nodiscard]]
    auto producer_cache_line() noexcept -> spsc_queue_cache_line &
    {
        return region_->producer_cache_line;
    }

    [[nodiscard]]
    auto producer_cache_line() const noexcept -> const spsc_queue_cache_line &
    {
        return region_->producer_cache_line;
    }

    [[nodiscard]]
    auto consumer_cache_line() noexcept -> spsc_queue_cache_line &
    {
        return region_->consumer_cache_line;
    }

    [[nodiscard]]
    auto consumer_cache_line() const noexcept -> const spsc_queue_cache_line &
    {
        return region_->consumer_cache_line;
    }

    // Return the slot at the given index.
    // The caller must ensure that the index is valid.
    [[nodiscard]]
    auto slot(std::size_t index) noexcept -> spsc_queue_slot &
    {
        return region_->slots[index];
    }

private:
    static auto _initialize_header(region & value, std::uint64_t schema_id) noexcept -> void
    {
        auto & header = value.header;
        header.magic = magic;
        header.version_major = version_major;
        header.version_minor = version_minor;
        header.header_size = sizeof(spsc_queue_header);
        header.region_size = sizeof(region);
        header.capacity = Capacity;
        header.slot_size = sizeof(T);
        header.slot_alignment = alignof(T);
        header.slot_stride = sizeof(spsc_queue_slot);
        header.slots_offset = offsetof(region, slots);
        header.schema_id = schema_id;
        header.state.store(ready_state, std::memory_order_release);
    }

    static auto
    _bind(std::span<std::byte> memory, spsc_queue_storage_mode mode, std::uint64_t schema_id)
        -> region *
    {
        if (memory.data() == nullptr || memory.size() < required_bytes()) {
            throw std::invalid_argument("spsc queue storage is null or too small");
        }
        if (reinterpret_cast<std::uintptr_t>(memory.data()) % required_alignment() != 0) {
            throw std::invalid_argument("spsc queue storage is not properly aligned");
        }

        if (mode == spsc_queue_storage_mode::initialize) {
            auto * value = ::new (static_cast<void *>(memory.data())) region;
            _initialize_header(*value, schema_id);
            return value;
        }
        if (mode != spsc_queue_storage_mode::attach) {
            throw std::invalid_argument("invalid spsc queue storage mode");
        }

        // Inspect the common header before treating the region as this specialization.
        const auto & header = *std::launder(reinterpret_cast<spsc_queue_header *>(memory.data()));
        if (header.state.load(std::memory_order_acquire) != ready_state) {
            throw std::invalid_argument("spsc queue storage is not ready");
        }
        if (header.magic != magic || header.version_major != version_major ||
            header.version_minor != version_minor ||
            header.header_size != sizeof(spsc_queue_header) ||
            header.region_size != sizeof(region) || header.capacity != Capacity ||
            header.slot_size != sizeof(T) || header.slot_alignment != alignof(T) ||
            header.slot_stride != sizeof(spsc_queue_slot) ||
            header.slots_offset != offsetof(region, slots) || header.schema_id != schema_id ||
            header.flags != 0) {
            throw std::invalid_argument("incompatible spsc queue storage header");
        }
        return std::launder(reinterpret_cast<region *>(memory.data()));
    }

private:
    region * region_;
    bool owns_memory_;
    bool owns_state_;
};

// SPSC Queue Producer.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue_producer final
{
private:
    friend class spsc_queue<T, Capacity>;
    using storage_type = spsc_queue_storage<T, Capacity>;
    using slot_type = typename storage_type::spsc_queue_slot;

    explicit spsc_queue_producer(storage_type & storage) noexcept
        : producer_cache_line_(&storage.producer_cache_line())
        , consumer_cache_line_(&storage.consumer_cache_line())
        , slots_(&storage.slot(0))
        , local_write_seq_(producer_cache_line_->seq.load(std::memory_order_relaxed))
        , cached_read_seq_(consumer_cache_line_->seq.load(std::memory_order_acquire))
    {}

public:
    spsc_queue_producer(const spsc_queue_producer &) = delete;
    auto operator=(const spsc_queue_producer &) -> spsc_queue_producer & = delete;
    spsc_queue_producer(spsc_queue_producer &&) = delete;
    auto operator=(spsc_queue_producer &&) -> spsc_queue_producer & = delete;

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

private:
    // Commit the local write sequence to the producer's cache line.
    auto _commit() noexcept -> void
    {
        ++local_write_seq_;
        producer_cache_line_->seq.store(local_write_seq_, std::memory_order_release);
    }

    template <typename... Args>
    auto _try_emplace(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> bool
        requires std::is_constructible_v<T, Args...>
    {
        // The producer owns its local write sequence; only refresh the peer's sequence when full.
        const auto write_seq = local_write_seq_;
        auto & cached_read_seq = cached_read_seq_;

        // Check if the queue is full.
        if (write_seq - cached_read_seq == Capacity) {
            // Slow path. Read and update the producer's cached read sequence.
            cached_read_seq = consumer_cache_line_->seq.load(std::memory_order_acquire);

            // Check if the queue is still full.
            if (write_seq - cached_read_seq == Capacity) {
                return false;
            }
        }

        // Fast path.
        // Construct the element in the aligned slot storage.
        auto * slot = slots_[write_seq & (Capacity - 1)].address();
        std::construct_at(slot, std::forward<Args>(args)...);

        // Publish the element only after construction succeeds.
        _commit();

        return true;
    }

private:
    spsc_queue_cache_line * producer_cache_line_;
    spsc_queue_cache_line * consumer_cache_line_;
    slot_type * slots_;
    std::uint64_t local_write_seq_;
    std::uint64_t cached_read_seq_;
};

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

// SPSC Queue.
template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_queue final
{
public:
    explicit spsc_queue(std::uint64_t schema_id = 0)
        : storage_(schema_id)
    {}

    explicit spsc_queue(
        std::span<std::byte> memory,
        spsc_queue_storage_mode mode,
        std::uint64_t schema_id = 0
    )
        : storage_(memory, mode, schema_id)
    {}

    spsc_queue(const spsc_queue &) = delete;

    auto operator=(const spsc_queue &) -> spsc_queue & = delete;

    spsc_queue(spsc_queue &&) = delete;

    auto operator=(spsc_queue &&) -> spsc_queue & = delete;

    ~spsc_queue() noexcept = default;

public:
    // Only one producer may be active for a region, including through other attached queues.
    // Retire the previous producer and synchronize the handoff before attaching a replacement.
    // Endpoints must not outlive the queue state owner. Attachment does not claim a shared lock.
    [[nodiscard]]
    auto attach_productor() noexcept -> spsc_queue_producer<T, Capacity>
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
    spsc_queue_storage<T, Capacity> storage_;
};

} // namespace pulse::spsc
