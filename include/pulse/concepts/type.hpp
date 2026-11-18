// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <type_traits>

namespace pulse::concepts
{

template <typename T>
concept IntraProcessElementType =
    std::is_object_v<T> && requires { sizeof(T); } && std::is_same_v<T, std::remove_cv_t<T>> &&
    !std::is_array_v<T> && !std::is_abstract_v<T> && std::is_nothrow_destructible_v<T>;

template <typename T>
concept InterProcessElementType =
    IntraProcessElementType<T> && std::is_standard_layout_v<T> && std::is_trivially_copyable_v<T> &&
    !std::is_pointer_v<T> && !std::is_member_pointer_v<T>;

} // namespace pulse::concepts
