// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <utility>

#include "pulse/concepts/type.hpp"

namespace pulse::storage
{

// Slot for the queue.
template <concepts::QueueElementType T>
class alignas(T) spsc_queue_slot final
{
private:
    // Inactive storage for one T. The slot starts and ends that lifetime with construct() and
    // destroy(); this union does neither. The member is the T itself, so a pointer or reference
    // to it designates the live object and does not need std::launder. A byte buffer would only
    // provide storage: after placement new, reinterpret_cast would not point at the new T.
    union storage_type final
    {
    public:
        storage_type() noexcept {}

        ~storage_type() noexcept {}

    public:
        T value;
    };

public:
    spsc_queue_slot() noexcept = default;

    spsc_queue_slot(const spsc_queue_slot &) noexcept = delete;

    spsc_queue_slot(spsc_queue_slot &&) noexcept = delete;

    auto operator=(const spsc_queue_slot &) noexcept -> spsc_queue_slot & = delete;

    auto operator=(spsc_queue_slot &&) noexcept -> spsc_queue_slot & = delete;

public:
    // Return the address of the slot.
    [[nodiscard]]
    auto address() noexcept -> T *
    {
        return std::addressof(storage.value);
    }

    // Only call when this slot contains a live T.
    [[nodiscard]]
    auto element() noexcept -> T &
    {
        return storage.value;
    }

    // Requires no live T in this slot. On success, starts its lifetime.
    template <typename... Args>
    auto construct(Args &&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) -> T &
        requires std::is_constructible_v<T, Args...>
    {
        return *std::construct_at(address(), std::forward<Args>(args)...);
    }

    // Requires a live T. Ends its lifetime; must not be called twice.
    auto destroy() noexcept -> void
    {
        std::destroy_at(address());
    }

private:
    storage_type storage;
};

} // namespace pulse::storage
