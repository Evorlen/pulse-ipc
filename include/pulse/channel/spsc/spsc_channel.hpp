// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

#include "pulse/channel/channel_error.hpp"
#include "pulse/channel/spsc/spsc_superblock.hpp"
#include "pulse/common/result.hpp"
#include "pulse/concepts/size.hpp"
#include "pulse/concepts/type.hpp"
#include "pulse/memory/mapped_region.hpp"
#include "pulse/memory/memory_error.hpp"
#include "pulse/memory/shared_memory.hpp"
#include "pulse/sync/initialization_lock.hpp"
#include "pulse/sync/sync_error.hpp"
#include "pulse/transport/spsc/spsc_consumer.hpp"
#include "pulse/transport/spsc/spsc_producer.hpp"
#include "pulse/transport/spsc/spsc_queue.hpp"

namespace pulse::channel::spsc
{

template <concepts::QueueElementType T, std::size_t Capacity>
    requires concepts::QueueCapacityType<Capacity>
class spsc_channel final
{
private:
    class construction_key final
    {
    private:
        friend class spsc_channel;

    private:
        construction_key() noexcept = default;
    };

    struct region_layout final
    {
        std::size_t queue_offset;
        std::size_t region_size;
    };

    static_assert(sizeof(T) <= std::numeric_limits<std::uint32_t>::max());
    static_assert(alignof(T) <= std::numeric_limits<std::uint32_t>::max());
    static_assert(sizeof(spsc_channel_superblock) <= std::numeric_limits<std::uint32_t>::max());

    static constexpr std::uint64_t magic_number = 0x50554C534543484E;
    static constexpr std::uint16_t version_major = 1;
    static constexpr std::uint16_t version_minor = 0;
    static constexpr auto ready_state = static_cast<std::uint8_t>(spsc_channel_state::Ready);

public:
    // In-place construction by result; only channel factories can create the key.
    explicit spsc_channel(
        construction_key,
        memory::mapped_region && region,
        transport::spsc::spsc_queue_storage<T, Capacity> * storage,
        spsc_channel_superblock * superblock
    ) noexcept
        : region_(std::move(region))
        , queue_(storage)
        , superblock_(superblock)
    {}

    spsc_channel(const spsc_channel &) = delete;

    auto operator=(const spsc_channel &) -> spsc_channel & = delete;

    // Moving transfers the mapping; existing source endpoints remain valid under the new owner.
    spsc_channel(spsc_channel && other) noexcept
        : region_(std::move(other.region_))
        , queue_(std::exchange(other.queue_.storage_, nullptr))
        , superblock_(std::exchange(other.superblock_, nullptr))
    {}

    // End all endpoints borrowing the destination's old mapping before assigning to it.
    auto operator=(spsc_channel && other) noexcept -> spsc_channel &
    {
        if (this != &other) {
            queue_.storage_ = std::exchange(other.queue_.storage_, nullptr);
            superblock_ = std::exchange(other.superblock_, nullptr);
            region_ = std::move(other.region_);
        }

        return *this;
    }

    // End this mapping's endpoints first. Local closure does not destroy or unlink shared state.
    // Remove the name explicitly with shared_memory::unlink when the application is done with it.
    ~spsc_channel() noexcept = default;

public:
    // Exclusively create a named region, initialize its state, then publish Ready.
    // schema_id is application-defined; zero opts out of application-specific type identification.
    [[nodiscard]]
    static auto create(std::string_view name, std::uint32_t schema_id = 0) -> common::result<spsc_channel>
    {
        const auto layout = _layout();
        if (!layout) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::RegionSizeOverflow});
        }

        auto initialization = _try_initialization_lock(name);
        if (!initialization) [[unlikely]] {
            return std::unexpected(initialization.error());
        }

        auto shared = memory::shared_memory::create(name, layout->region_size);
        if (!shared) [[unlikely]] {
            return std::unexpected(shared.error());
        }

        auto region = shared->map();
        if (!region) [[unlikely]] {
            return _rollback_create(name, region.error());
        }

        auto bytes = region->bytes();
        if (reinterpret_cast<std::uintptr_t>(bytes.data()) % alignof(spsc_channel_superblock) != 0)
            [[unlikely]] {
            return _rollback_create(name, common::error {channel_error_code::RegionNotAligned});
        }

        auto * superblock = ::new (static_cast<void *>(bytes.data())) spsc_channel_superblock;
        auto storage = transport::spsc::spsc_queue<T, Capacity>::_initialize_storage(
            bytes.subspan(layout->queue_offset, transport::spsc::spsc_queue<T, Capacity>::required_bytes())
        );
        if (!storage) [[unlikely]] {
            return _rollback_create(name, storage.error());
        }

        superblock->version_major = version_major;
        superblock->version_minor = version_minor;
        superblock->superblock_size = sizeof(spsc_channel_superblock);
        superblock->region_size = layout->region_size;
        superblock->queue_offset = layout->queue_offset;
        superblock->capacity = Capacity;
        superblock->element_size = sizeof(T);
        superblock->element_alignment = alignof(T);
        superblock->schema_id = schema_id;
        // A recognizable magic must never precede construction of the shared objects.
        // Open can inspect these bytes under the external lock even after an initializer dies.
        std::atomic_thread_fence(std::memory_order_release);
        superblock->magic_number = magic_number;
        superblock->state.store(ready_state, std::memory_order_release);

        return common::result<spsc_channel>(
            std::in_place,
            construction_key {},
            std::move(*region),
            *storage,
            superblock
        );
    }

    // Bind an initialized region without reconstructing or clearing shared objects.
    // Open does not wait for the initialization lock: contention returns sync_error_code::Busy.
    [[nodiscard]]
    static auto open(std::string_view name, std::uint32_t schema_id = 0) -> common::result<spsc_channel>
    {
        const auto layout = _layout();
        if (!layout) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::RegionSizeOverflow});
        }

        auto initialization = _try_initialization_lock(name);
        if (!initialization) [[unlikely]] {
            return std::unexpected(initialization.error());
        }

        auto shared = memory::shared_memory::open(name);
        if (!shared) [[unlikely]] {
            return std::unexpected(shared.error());
        }

        auto region = shared->map();
        if (!region) [[unlikely]] {
            return std::unexpected(region.error());
        }

        auto bytes = region->bytes();
        if (bytes.size() < sizeof(spsc_channel_superblock)) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::RegionTooSmall});
        }
        if (reinterpret_cast<std::uintptr_t>(bytes.data()) % alignof(spsc_channel_superblock) != 0)
            [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::RegionNotAligned});
        }

        // Do not access atomic members until the constructor-completion marker is present.
        std::uint64_t observed_magic;
        std::memcpy(&observed_magic, bytes.data(), sizeof(observed_magic));
        if (observed_magic == 0) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::NotReady});
        }
        if (observed_magic != magic_number) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::MagicNumberMismatch});
        }

        auto * superblock = std::launder(reinterpret_cast<spsc_channel_superblock *>(bytes.data()));
        if (superblock->state.load(std::memory_order_acquire) != ready_state) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::NotReady});
        }
        auto valid = _validate_superblock(*superblock, bytes.size(), *layout, schema_id);
        if (!valid) [[unlikely]] {
            return std::unexpected(valid.error());
        }

        auto * address = bytes.data() + layout->queue_offset;
        if (reinterpret_cast<std::uintptr_t>(address) %
                transport::spsc::spsc_queue<T, Capacity>::required_alignment() !=
            0) [[unlikely]] {
            return std::unexpected(common::error {transport::spsc::spsc_error_code::StorageNotAligned});
        }
        auto * storage =
            std::launder(reinterpret_cast<transport::spsc::spsc_queue_storage<T, Capacity> *>(address));

        return common::result<spsc_channel>(
            std::in_place,
            construction_key {},
            std::move(*region),
            storage,
            superblock
        );
    }

    [[nodiscard]]
    auto try_attach_producer() noexcept -> std::optional<transport::spsc::spsc_queue_producer<T, Capacity>>
    {
        if (superblock_ == nullptr) [[unlikely]] {
            return std::nullopt;
        }

        bool expected = false;
        const auto success = superblock_->producer_attached.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acquire,
            std::memory_order_relaxed
        );
        if (!success) {
            return std::nullopt;
        }

        return queue_._try_attach_producer(&superblock_->producer_attached);
    }

    [[nodiscard]]
    auto try_attach_consumer() noexcept -> std::optional<transport::spsc::spsc_queue_consumer<T, Capacity>>
    {
        if (superblock_ == nullptr) [[unlikely]] {
            return std::nullopt;
        }

        bool expected = false;
        const auto success = superblock_->consumer_attached.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acquire,
            std::memory_order_relaxed
        );
        if (!success) {
            return std::nullopt;
        }

        return queue_._try_attach_consumer(&superblock_->consumer_attached);
    }

private:
    [[nodiscard]]
    static auto _try_initialization_lock(std::string_view name) -> common::result<sync::initialization_lock>
    {
        auto lock = sync::initialization_lock::try_acquire(name);
        if (!lock) {
            // Keep existing channel name errors in the memory domain.
            const auto code = lock.error().encode();
            if (code == common::error {sync::sync_error_code::NameInvalid}.encode()) {
                return std::unexpected(common::error {memory::memory_error_code::NameInvalid});
            }
            if (code == common::error {sync::sync_error_code::NameTooLong}.encode()) {
                return std::unexpected(common::error {memory::memory_error_code::NameTooLong});
            }
            return std::unexpected(lock.error());
        }
        return lock;
    }

    [[nodiscard]]
    static constexpr auto _layout() noexcept -> std::optional<region_layout>
    {
        constexpr auto alignment = transport::spsc::spsc_queue<T, Capacity>::required_alignment();
        constexpr auto header_size = sizeof(spsc_channel_superblock);
        constexpr auto padding = (alignment - header_size % alignment) % alignment;
        constexpr auto maximum = std::numeric_limits<std::size_t>::max();
        if (padding > maximum - header_size) [[unlikely]] {
            return std::nullopt;
        }
        const auto offset = header_size + padding;
        if (transport::spsc::spsc_queue<T, Capacity>::required_bytes() > maximum - offset) [[unlikely]] {
            return std::nullopt;
        }
        return region_layout {offset, offset + transport::spsc::spsc_queue<T, Capacity>::required_bytes()};
    }

    [[nodiscard]]
    static auto _validate_superblock(
        const spsc_channel_superblock & header,
        std::size_t mapped_size,
        region_layout layout,
        std::uint32_t schema_id
    ) noexcept -> common::status
    {
        if (header.magic_number != magic_number) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::MagicNumberMismatch});
        }
        if (header.version_major != version_major || header.version_minor != version_minor) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::VersionMismatch});
        }
        if (header.superblock_size != sizeof(spsc_channel_superblock)) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::SuperblockSizeMismatch});
        }
        if (header.capacity != Capacity) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::CapacityMismatch});
        }
        if (header.element_size != sizeof(T)) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::ElementSizeMismatch});
        }
        if (header.element_alignment != alignof(T)) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::ElementAlignmentMismatch});
        }
        if (header.schema_id != schema_id) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::SchemaMismatch});
        }
        if (header.flags != 0) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::UnsupportedFlags});
        }
        if (header.queue_offset != layout.queue_offset) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::QueueOffsetMismatch});
        }
        if (header.region_size != mapped_size || header.region_size != layout.region_size) [[unlikely]] {
            return std::unexpected(common::error {channel_error_code::RegionSizeMismatch});
        }
        return {};
    }

    // Unlink only an object created by this attempt. Preserve the primary error if cleanup fails.
    [[nodiscard]]
    static auto _rollback_create(std::string_view name, common::error error) -> common::result<spsc_channel>
    {
        static_cast<void>(memory::shared_memory::unlink(name));
        return std::unexpected(error);
    }

private:
    // Destroy the local queue view before unmapping its storage.
    memory::mapped_region region_;
    transport::spsc::spsc_queue<T, Capacity> queue_;
    spsc_channel_superblock * superblock_;
};

} // namespace pulse::channel::spsc
