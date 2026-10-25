// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <type_traits>

namespace pulse::concepts
{

template <typename T>
concept QueueElementType =
    std::is_object_v<T> && std::is_same_v<T, std::remove_cv_t<T>> &&
    std::is_standard_layout_v<T> &&
    std::is_nothrow_destructible_v<T> &&
    !std::is_array_v<T>;

} // namespace pulse::concepts
