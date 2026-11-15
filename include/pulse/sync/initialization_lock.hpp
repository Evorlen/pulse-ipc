// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string_view>

#include "pulse/common/result.hpp"

namespace pulse::sync
{

// An exclusive, nonblocking initialization lease for a named channel.
// Each acquisition independently opens the lock file, including within the same process.
// Closing releases the lease but never unlinks its persistent lock file.
class initialization_lock final
{
public:
    initialization_lock(const initialization_lock &) = delete;

    auto operator=(const initialization_lock &) -> initialization_lock & = delete;

    initialization_lock(initialization_lock &&) noexcept;

    auto operator=(initialization_lock &&) noexcept -> initialization_lock &;

    ~initialization_lock() noexcept;

private:
    explicit initialization_lock(int fd) noexcept;

public:
    // Linux: leading slashes are ignored, matching the shared-memory namespace.
    // Busy means another participating thread/process owns the initialization lease.
    [[nodiscard]]
    static auto try_acquire(std::string_view name) -> common::result<initialization_lock>;

    auto close() noexcept -> void;

private:
    int fd_ {-1};
};

} // namespace pulse::sync
