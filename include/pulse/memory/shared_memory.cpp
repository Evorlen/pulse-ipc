// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include "pulse/memory/shared_memory.hpp"

#include <cerrno>
#include <expected>
#include <string>
#include <utility>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#error "Unsupported platform"
#endif

#include "pulse/memory/memory_error.hpp"

namespace pulse::memory
{

shared_memory::shared_memory(shared_memory && other) noexcept
    : fd_(std::exchange(other.fd_, invalid_fd))
    , size_(std::exchange(other.size_, std::size_t {0}))
{}

auto shared_memory::operator=(shared_memory && other) noexcept -> shared_memory &
{
    if (this != &other) {
        shared_memory incoming(std::move(other));
        std::swap(fd_, incoming.fd_);
        std::swap(size_, incoming.size_);
    }

    return *this;
}

shared_memory::~shared_memory() noexcept
{
    close();
}

shared_memory::shared_memory(handle_type handle, std::size_t size) noexcept
    : fd_(handle)
    , size_(size)
{}

auto shared_memory::create(std::string_view name, std::size_t size) -> common::result<shared_memory>
{
    if (size == 0) {
        return std::unexpected(common::error {memory_error_code::SizeInvalid});
    }

    if (name.empty() || name.find('\0') != std::string_view::npos) {
        return std::unexpected(common::error {memory_error_code::NameInvalid});
    }
    const std::string native_name(name);

#if defined(__linux__)
    if (!std::in_range<off_t>(size)) {
        return std::unexpected(common::error {memory_error_code::SizeTooLarge});
    }

    int fd {-1};
    do {
        fd = ::shm_open(native_name.c_str(), O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    } while (fd == -1 && errno == EINTR);

    if (fd == -1) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case EEXIST:
            error_code = memory_error_code::AlreadyExists;
            break;
        case EACCES:
            error_code = memory_error_code::PermissionDenied;
            break;
        case EINVAL:
            error_code = memory_error_code::NameInvalid;
            break;
        case ENAMETOOLONG:
            error_code = memory_error_code::NameTooLong;
            break;
        case EMFILE:
        case ENFILE:
            error_code = memory_error_code::HandleLimitReached;
            break;
        case ENOMEM:
        case ENOSPC:
            error_code = memory_error_code::ResourceExhausted;
            break;
        default:
            error_code = memory_error_code::CreateFailed;
        }

        return std::unexpected(common::error {error_code});
    }

    const auto native_size = static_cast<off_t>(size);

    int res {-1};
    do {
        res = ::ftruncate(fd, native_size);
    } while (res == -1 && errno == EINTR);

    if (res == -1) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case EFBIG:
            error_code = memory_error_code::SizeTooLarge;
            break;
        case ENOSPC:
        case ENOMEM:
            error_code = memory_error_code::ResourceExhausted;
            break;
        default:
            error_code = memory_error_code::SetSizeFailed;
            break;
        }

        // Try to rollback and clean up the resources.
        int unlink_result;
        do {
            unlink_result = ::shm_unlink(native_name.c_str());
        } while (unlink_result == -1 && errno == EINTR);

        static_cast<void>(::close(fd));

        return std::unexpected(common::error {error_code});
    }

    return shared_memory {fd, size};
#else
#error "Unsupported platform"
#endif
}

auto shared_memory::open(std::string_view name) -> common::result<shared_memory>
{
    if (name.empty() || name.find('\0') != std::string_view::npos) {
        return std::unexpected(common::error {memory_error_code::NameInvalid});
    }
    const std::string native_name(name);

#if defined(__linux__)
    int fd {-1};
    do {
        fd = ::shm_open(native_name.c_str(), O_RDWR, 0);
    } while (fd == -1 && errno == EINTR);

    if (fd == -1) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case ENOENT:
            error_code = memory_error_code::NotFound;
            break;
        case EACCES:
            error_code = memory_error_code::PermissionDenied;
            break;
        case EINVAL:
            error_code = memory_error_code::NameInvalid;
            break;
        case ENAMETOOLONG:
            error_code = memory_error_code::NameTooLong;
            break;
        case EMFILE:
        case ENFILE:
            error_code = memory_error_code::HandleLimitReached;
            break;
        case ENOMEM:
        case ENOSPC:
            error_code = memory_error_code::ResourceExhausted;
            break;
        default:
            error_code = memory_error_code::OpenFailed;
        }

        return std::unexpected(common::error {error_code});
    }

    struct stat status {};

    int res {-1};
    do {
        res = ::fstat(fd, &status);
    } while (res == -1 && errno == EINTR);

    if (res == -1) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case ENOMEM:
            error_code = memory_error_code::ResourceExhausted;
            break;
        default:
            error_code = memory_error_code::GetSizeFailed;
            break;
        }

        static_cast<void>(::close(fd));

        return std::unexpected(common::error {error_code});
    }

    // A creator may have published the name before finishing ftruncate.
    // Reject zero-sized objects; callers coordinate initialization separately.
    if (status.st_size <= 0) {
        static_cast<void>(::close(fd));

        return std::unexpected(common::error {memory_error_code::SizeInvalid});
    }

    if (!std::in_range<std::size_t>(status.st_size)) {
        static_cast<void>(::close(fd));

        return std::unexpected(common::error {memory_error_code::SizeTooLarge});
    }

    return shared_memory {fd, static_cast<std::size_t>(status.st_size)};
#else
#error "Unsupported platform"
#endif
}

auto shared_memory::map() const noexcept -> common::result<mapped_region>
{
#if defined(__linux__)
    if (fd_ == invalid_fd) {
        return std::unexpected(common::error {memory_error_code::HandleInvalid});
    }

    if (size_ == 0) {
        return std::unexpected(common::error {memory_error_code::SizeInvalid});
    }

    void * address;
    do {
        address = ::mmap(
            nullptr,
            size_,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd_,
            0
        );
    } while (address == MAP_FAILED && errno == EINTR);

    if (address == MAP_FAILED) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case EBADF:
            error_code = memory_error_code::HandleInvalid;
            break;
        case EACCES:
        case EPERM:
            error_code = memory_error_code::PermissionDenied;
            break;
        case ENOMEM:
            error_code = memory_error_code::ResourceExhausted;
            break;
        case ENFILE:
            error_code = memory_error_code::HandleLimitReached;
            break;
        case EOVERFLOW:
            error_code = memory_error_code::SizeTooLarge;
            break;
        default:
            error_code = memory_error_code::MapFailed;
            break;
        }

        return std::unexpected(common::error {error_code});
    }

    return mapped_region {address, size_};
#else
#error "Unsupported platform"
#endif
}

void shared_memory::close() noexcept
{
#if defined(__linux__)
    if (fd_ != invalid_fd) {
        static_cast<void>(::close(fd_));
        fd_ = invalid_fd;
        size_ = 0;
    }
#else
#error "Unsupported platform"
#endif
}

auto shared_memory::unlink(std::string_view name) -> common::status
{
    if (name.empty() || name.find('\0') != std::string_view::npos) {
        return std::unexpected(common::error {memory_error_code::NameInvalid});
    }

    const std::string native_name(name);

#if defined(__linux__)
    int res {-1};
    do {
        res = ::shm_unlink(native_name.c_str());
    } while (res == -1 && errno == EINTR);

    if (res == -1) {
        const int native_error = errno;

        memory_error_code error_code;
        switch (native_error) {
        case ENOENT:
            error_code = memory_error_code::NotFound;
            break;
        case EACCES:
        case EPERM:
            error_code = memory_error_code::PermissionDenied;
            break;
        case ENAMETOOLONG:
            error_code = memory_error_code::NameTooLong;
            break;
        case EINVAL:
            error_code = memory_error_code::NameInvalid;
            break;
        default:
            error_code = memory_error_code::UnlinkFailed;
            break;
        }

        return std::unexpected(common::error {error_code});
    }

    return {};
#else
#error "Unsupported platform"
#endif
}

auto shared_memory::size() const noexcept -> std::size_t
{
    return size_;
}

} // namespace pulse::memory
