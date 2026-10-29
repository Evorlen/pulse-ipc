// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <concepts>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace pulse::common
{

// Categories of errors.
// All types of errors must be registered here with unique values.
enum class error_category : std::uint8_t
{
    Unknown = 0,
    SPSC = 1,
};

// Traits for error types.
// When a new error type is defined, a explicit specialization of this struct must be provided.
// For example:
// template<>
// struct error_traits<my_error_type>
// {
//     static constexpr error_category category = error_category::MyError;
// };
template <typename E>
struct error_traits;

// Concept for error types.
// A valid error type must be an enum type with underlying type std::uint8_t
// and a corresponding error_traits specialization.
template <typename E>
concept ErrorType =
    std::is_enum_v<E> && std::same_as<std::underlying_type_t<E>, std::uint8_t> && requires {
        { error_traits<E>::category } -> std::same_as<const error_category &>;
    };

// Error class.
// It provides encode() to combine category and code into a 16-bit value.
class error final
{
public:
    template <ErrorType E>
    explicit error(E code) noexcept
        : category_(error_traits<E>::category)
        , code_(std::to_underlying(code))
    {}

public:
    [[nodiscard]]
    auto category() const noexcept -> error_category
    {
        return category_;
    }

    [[nodiscard]]
    auto code() const noexcept -> std::uint8_t
    {
        return code_;
    }

    [[nodiscard]]
    auto encode() const noexcept -> std::uint16_t
    {
        return static_cast<std::uint16_t>(category_) << 8 | code_;
    }

private:
    error_category category_;
    std::uint8_t code_;
};

} // namespace pulse::common
