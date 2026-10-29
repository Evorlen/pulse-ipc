// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "pulse/common/error.hpp"

namespace pulse::spsc
{

// Error codes for the SPSC queue.
enum class spsc_error_code : std::uint8_t
{
    StorageNull = 1,
    StorageTooSmall = 2,
    StorageNotAligned = 3,
    StorageInvalidMode = 4,
    StorageNotReady = 5,
    StorageIncompatibleHeader = 6,
    StorageAllocationFailed = 7,
};

} // namespace pulse::spsc

namespace pulse::common
{

template <>
struct error_traits<spsc::spsc_error_code>
{
    static constexpr error_category category = error_category::SPSC;
};

} // namespace pulse::common
