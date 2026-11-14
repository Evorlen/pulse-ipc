// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "pulse/common/error.hpp"

namespace pulse::transport::spsc
{

// Error codes for the SPSC queue.
enum class spsc_error_code : std::uint8_t
{
    StorageNull = 1,
    StorageTooSmall = 2,
    StorageNotAligned = 3,
    StorageInvalidMode = 4,
    StorageAllocationFailed = 5,
};

} // namespace pulse::transport::spsc

namespace pulse::common
{

template <>
struct error_traits<transport::spsc::spsc_error_code>
{
    static constexpr error_category category = error_category::SPSC;
};

} // namespace pulse::common
