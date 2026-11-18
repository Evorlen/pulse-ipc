// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

static_assert(std::atomic<std::uint8_t>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);

namespace pulse::channel::spsc
{

struct spsc_channel_superblock final
{
    std::uint64_t magic_number {0};
    std::uint16_t version_major {0};
    std::uint16_t version_minor {0};
    std::uint32_t superblock_size {0};
    std::uint64_t region_size {0};
    std::uint64_t queue_offset {0};
    std::uint64_t capacity {0};
    std::uint32_t element_size {0};
    std::uint32_t element_alignment {0};
    std::uint32_t schema_id {0};
    std::uint32_t flags {0};
    std::atomic<std::uint8_t> state {0};
    std::atomic<bool> producer_attached {false};
    std::atomic<bool> consumer_attached {false};
    std::array<std::byte, 64> reserved {};
};

static_assert(std::is_standard_layout_v<spsc_channel_superblock>);

enum class spsc_channel_state : std::uint8_t
{
    Initializing = 0,
    Ready = 1,
};

} // namespace pulse::channel::spsc
