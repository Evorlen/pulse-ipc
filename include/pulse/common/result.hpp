// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <expected>

#include "pulse/common/error.hpp"

namespace pulse::common
{

template <typename T>
using result = std::expected<T, error>;

using status = std::expected<void, error>;

} // namespace pulse::common
