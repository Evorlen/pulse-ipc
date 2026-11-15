// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "pulse/common/error.hpp"

namespace pulse::channel
{

// Errors from channel layout and protocol validation.
// Mapping and queue-storage failures stay in memory_error_code and spsc_error_code.
enum class channel_error_code : std::uint8_t
{
    RegionTooSmall = 1,
    RegionNotAligned = 2,
    RegionSizeOverflow = 3,
    NotReady = 4,
    MagicNumberMismatch = 5,
    VersionMismatch = 6,
    SuperblockSizeMismatch = 7,
    RegionSizeMismatch = 8,
    QueueOffsetMismatch = 9,
    CapacityMismatch = 10,
    ElementSizeMismatch = 11,
    ElementAlignmentMismatch = 12,
    SchemaMismatch = 13,
    UnsupportedFlags = 14,
};

} // namespace pulse::channel

namespace pulse::common
{

template <>
struct error_traits<channel::channel_error_code>
{
    static constexpr error_category category = error_category::Channel;
};

} // namespace pulse::common
