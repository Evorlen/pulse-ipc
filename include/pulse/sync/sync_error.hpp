// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "pulse/common/error.hpp"

namespace pulse::sync
{

enum class sync_error_code : std::uint8_t
{
    NameInvalid = 1,
    NameTooLong = 2,
    PermissionDenied = 3,
    HandleLimitReached = 4,
    ResourceExhausted = 5,
    LockDirectoryCreateFailed = 6,
    LockDirectoryOpenFailed = 7,
    LockDirectoryInvalid = 8,
    LockFileOpenFailed = 9,
    LockFileInvalid = 10,
    Busy = 11,
    LockAcquireFailed = 12,
};

} // namespace pulse::sync

namespace pulse::common
{

template <>
struct error_traits<sync::sync_error_code>
{
    static constexpr error_category category = error_category::Sync;
};

} // namespace pulse::common
