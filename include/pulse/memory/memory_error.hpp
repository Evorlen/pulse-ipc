// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "pulse/common/error.hpp"

namespace pulse::memory
{

// Error codes for memory mapping.
enum class memory_error_code : std::uint8_t
{
    SizeInvalid = 1,
    SizeTooLarge = 2,
    NameInvalid = 3,
    NameTooLong = 4,
    AlreadyExists = 5,
    PermissionDenied = 6,
    HandleLimitReached = 7,
    ResourceExhausted = 8,
    CreateFailed = 9,
    SetSizeFailed = 10,
    NotFound = 11,
    GetSizeFailed = 12,
    OpenFailed = 13,
    HandleInvalid = 14,
    MapFailed = 15,
    UnlinkFailed = 16,
};

} // namespace pulse::memory

namespace pulse::common
{

template <>
struct error_traits<memory::memory_error_code>
{
    static constexpr error_category category = error_category::Memory;
};

} // namespace pulse::common
