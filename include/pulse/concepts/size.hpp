// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <bit>
#include <cstddef>

namespace pulse::concepts
{

template <std::size_t Capacity>
concept QueueCapacityType = Capacity >= 2 && std::has_single_bit(Capacity);

} // namespace pulse::concepts
