// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include "pulse/sync/initialization_lock.hpp"

#include <cerrno>
#include <cstdint>
#include <expected>
#include <string>
#include <utility>

#if defined(__linux__)
#include <fcntl.h>
#include <limits.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#else
#error "Unsupported platform"
#endif

#include "pulse/sync/sync_error.hpp"

namespace pulse::sync
{
namespace
{

class file_descriptor final
{
public:
    explicit file_descriptor(int fd) noexcept
        : fd_(fd)
    {}

    file_descriptor(const file_descriptor &) = delete;

    auto operator=(const file_descriptor &) -> file_descriptor & = delete;

    ~file_descriptor() noexcept
    {
        if (fd_ != -1) {
            static_cast<void>(::close(fd_));
        }
    }

public:
    [[nodiscard]]
    auto get() const noexcept -> int
    {
        return fd_;
    }

    [[nodiscard]]
    auto release() noexcept -> int
    {
        return std::exchange(fd_, -1);
    }

private:
    int fd_;
};

auto native_error(int code, sync_error_code fallback) noexcept -> common::error
{
    switch (code) {
    case EACCES:
    case EPERM:
        return common::error {sync_error_code::PermissionDenied};
    case ENAMETOOLONG:
        return common::error {sync_error_code::NameTooLong};
    case EMFILE:
    case ENFILE:
        return common::error {sync_error_code::HandleLimitReached};
    case ENOMEM:
    case ENOSPC:
    case EDQUOT:
    case ENOLCK:
        return common::error {sync_error_code::ResourceExhausted};
    default:
        return common::error {fallback};
    }
}

// FNV-1a is fixed here rather than std::hash, whose mapping is not a protocol guarantee.
// A collision only serializes unrelated channels; the shared-memory names remain distinct.
[[nodiscard]]
auto lock_file_name(std::string_view name) -> std::string
{
    std::uint64_t hash = 14695981039346656037ULL;
    for (char byte : name) {
        hash ^= static_cast<unsigned char>(byte);
        hash *= 1099511628211ULL;
    }

    constexpr char digits[] = "0123456789abcdef";
    std::string filename {"init-"};
    for (int shift = 60; shift >= 0; shift -= 4) {
        filename += digits[(hash >> shift) & 0xf];
    }
    filename += ".lock";
    return filename;
}

[[nodiscard]]
auto stat_file(int fd, struct stat & status) noexcept -> int
{
    int result;
    do {
        result = ::fstat(fd, &status);
    } while (result == -1 && errno == EINTR);
    return result;
}

} // namespace

initialization_lock::initialization_lock(int fd) noexcept
    : fd_(fd)
{}

initialization_lock::initialization_lock(initialization_lock && other) noexcept
    : fd_(std::exchange(other.fd_, -1))
{}

auto initialization_lock::operator=(initialization_lock && other) noexcept -> initialization_lock &
{
    if (this != &other) {
        initialization_lock incoming(std::move(other));
        std::swap(fd_, incoming.fd_);
    }
    return *this;
}

initialization_lock::~initialization_lock() noexcept
{
    close();
}

auto initialization_lock::try_acquire(std::string_view name) -> common::result<initialization_lock>
{
    if (name.empty() || name.find('\0') != std::string_view::npos) [[unlikely]] {
        return std::unexpected(common::error {sync_error_code::NameInvalid});
    }
    while (name.starts_with('/')) {
        name.remove_prefix(1);
    }
    if (name.empty() || name.find('/') != std::string_view::npos || name == "." || name == "..")
        [[unlikely]] {
        return std::unexpected(common::error {sync_error_code::NameInvalid});
    }
    if (name.size() > NAME_MAX) [[unlikely]] {
        return std::unexpected(common::error {sync_error_code::NameTooLong});
    }

    const auto owner = ::geteuid();
    const auto directory_name = std::string("/tmp/pulse-ipc-init-") + std::to_string(owner);
    int result;
    do {
        result = ::mkdir(directory_name.c_str(), S_IRWXU);
    } while (result == -1 && errno == EINTR);
    if (result == -1 && errno != EEXIST) [[unlikely]] {
        return std::unexpected(native_error(errno, sync_error_code::LockDirectoryCreateFailed));
    }

    int directory_fd;
    do {
        directory_fd = ::open(directory_name.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    } while (directory_fd == -1 && errno == EINTR);
    if (directory_fd == -1) [[unlikely]] {
        return std::unexpected(native_error(errno, sync_error_code::LockDirectoryOpenFailed));
    }
    const file_descriptor directory {directory_fd};
    struct stat status {};
    if (stat_file(directory.get(), status) == -1) [[unlikely]] {
        return std::unexpected(native_error(errno, sync_error_code::LockDirectoryInvalid));
    }
    if (!S_ISDIR(status.st_mode) || status.st_uid != owner || (status.st_mode & (S_IRWXG | S_IRWXO)) != 0)
        [[unlikely]] {
        return std::unexpected(common::error {sync_error_code::LockDirectoryInvalid});
    }

    const auto filename = lock_file_name(name);
    int lock_fd;
    do {
        lock_fd = ::openat(
            directory.get(),
            filename.c_str(),
            O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK,
            S_IRUSR | S_IWUSR
        );
    } while (lock_fd == -1 && errno == EINTR);
    if (lock_fd == -1) [[unlikely]] {
        return std::unexpected(native_error(errno, sync_error_code::LockFileOpenFailed));
    }
    file_descriptor file {lock_fd};
    if (stat_file(file.get(), status) == -1) [[unlikely]] {
        return std::unexpected(native_error(errno, sync_error_code::LockFileInvalid));
    }
    if (!S_ISREG(status.st_mode) || status.st_uid != owner || (status.st_mode & (S_IRWXG | S_IRWXO)) != 0)
        [[unlikely]] {
        return std::unexpected(common::error {sync_error_code::LockFileInvalid});
    }

    do {
        result = ::flock(file.get(), LOCK_EX | LOCK_NB);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return std::unexpected(common::error {sync_error_code::Busy});
        }
        return std::unexpected(native_error(errno, sync_error_code::LockAcquireFailed));
    }

    return initialization_lock {file.release()};
}

auto initialization_lock::close() noexcept -> void
{
    const auto fd = std::exchange(fd_, -1);
    if (fd != -1) {
        int result;
        do {
            result = ::flock(fd, LOCK_UN);
        } while (result == -1 && errno == EINTR);
        static_cast<void>(::close(fd));
    }
}

} // namespace pulse::sync
