// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <bit>
#include <cstddef>
#include <limits>

namespace pulse::concepts
{

template <std::size_t Capacity>
concept QueueCapacityType = Capacity > 0 &&
                            Capacity <= std::bit_floor(std::numeric_limits<std::size_t>::max());

} // namespace pulse::concepts
