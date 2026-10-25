// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>

namespace pulse::config
{

// The size of a cache line in bytes.
// For 64-bit architectures, this is usually 64 bytes. If you are using
// a different architecture, you can change this value. For example, 32.
// This value will influence the alignment of data structures in
// this library, which may affect performance. After changing this value,
// you will need to rebuild the library and test the performance.
constexpr std::size_t cache_line_alignment = 64;

} // namespace pulse::config
