// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <latch>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <print>
#include <span>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#if defined(__linux__)
#include <sched.h>
#else
#error "Unsupported platform"
#endif

#include <benchmark/benchmark.h>
#include <pulse/transport/spsc/spsc_queue.hpp>

struct cpu_pair
{
    int first;
    int second;
};

class scoped_cpu_affinity final
{
public:
    explicit scoped_cpu_affinity(int cpu) noexcept
        : pinned_(::sched_getaffinity(0, sizeof(previous_), &previous_) == 0 && pin_current_thread(cpu))
    {}

    scoped_cpu_affinity(const scoped_cpu_affinity &) = delete;

    auto operator=(const scoped_cpu_affinity &) -> scoped_cpu_affinity & = delete;

    ~scoped_cpu_affinity()
    {
        if (pinned_) {
            ::sched_setaffinity(0, sizeof(previous_), &previous_);
        }
    }

public:
    static auto pin_current_thread(int cpu) noexcept -> bool
    {
        cpu_set_t mask {};
        CPU_SET(cpu, &mask);
        return ::sched_setaffinity(0, sizeof(mask), &mask) == 0;
    }

    [[nodiscard]]
    auto pinned() const noexcept -> bool
    {
        return pinned_;
    }

private:
    cpu_set_t previous_ {};
    bool pinned_;
};

auto print_help() -> void
{
    std::fputs(
        "Usage: spsc_queue_throughput [options]\n"
        "Options:\n"
        "  --cpus=<producer>,<consumer>  Bind the threads to two distinct Linux logical CPUs.\n"
        "                                Omit to use system scheduling.\n"
        "  --help                        Print this help message and exit.\n\n"
        "Benchmark Options:\n",
        stdout
    );
    benchmark::PrintDefaultHelp();
}

auto parse_cpu_id(std::string_view value) -> std::optional<int>
{
    unsigned int cpu {0};
    const auto end = value.data() + value.size();
    const auto [parsed_end, error] = std::from_chars(value.data(), end, cpu);
    if (error != std::errc() || parsed_end != end) {
        return std::nullopt;
    }

#if defined(__linux__)
    if (cpu >= CPU_SETSIZE) {
        std::fputs("CPU ID is too large.\n", stderr);
        return std::nullopt;
    }
    cpu_set_t allowed {};
    if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
        std::fputs("sched_getaffinity failed.\n", stderr);
        return std::nullopt;
    }
    if (!CPU_ISSET(cpu, &allowed)) {
        std::println(stderr, "CPU {} is not allowed by the current affinity mask.", cpu);
        return std::nullopt;
    }
#else
#error "Unsupported platform"
#endif

    return static_cast<int>(cpu);
}

auto parse_args(int argc, std::span<char *> argv) -> std::optional<cpu_pair>
{
    constexpr std::string_view cpus_option = "--cpus=";

    if (argc == 1) {
        return std::nullopt;
    }
    if (argc != 2 || !std::string_view(argv[1]).starts_with(cpus_option)) {
        std::fputs("Expected --cpus arguments. Run with --help for usage help.\n", stderr);
        return std::nullopt;
    }

    const auto value = std::string_view(argv[1]).substr(cpus_option.size());
    const auto separator = value.find(',');
    if (separator == std::string_view::npos) {
        std::fputs("Invalid --cpus arguments format. Run with --help for usage help.\n", stderr);
        return std::nullopt;
    }
    const auto first = parse_cpu_id(value.substr(0, separator));
    if (!first) {
        std::fputs("Invalid producer CPU ID.\n", stderr);
        return std::nullopt;
    }
    const auto second = parse_cpu_id(value.substr(separator + 1));
    if (!second) {
        std::fputs("Invalid consumer CPU ID.\n", stderr);
        return std::nullopt;
    }
    if (*first == *second) {
        std::fputs("Producer and consumer CPU IDs must be distinct.\n", stderr);
        return std::nullopt;
    }

    return cpu_pair {.first = *first, .second = *second};
}

constexpr auto expected_checksum(std::uint64_t messages) noexcept -> std::uint64_t
{
    // Sum complete uint32_t sequence cycles and the remaining prefix, modulo 2^64.
    constexpr auto cycle = std::uint64_t {1} << std::numeric_limits<std::uint32_t>::digits;
    constexpr auto cycle_sum = (cycle - 1) * (cycle / 2);
    const auto tail = messages % cycle;
    return (messages / cycle) * cycle_sum + (tail * tail - tail) / 2;
}

template <std::size_t Capacity>
auto throughput(benchmark::State & state, std::optional<cpu_pair> cpus) -> void
try {
    std::optional<scoped_cpu_affinity> consumer_affinity;
    if (cpus) {
        consumer_affinity.emplace(cpus->second);
        if (!consumer_affinity->pinned()) {
            state.SkipWithError("Failed to pin the consumer thread");
            return;
        }
    }
    state.SetLabel(cpus ? std::format("cpu {} -> {}", cpus->first, cpus->second) : "system scheduling");

    using queue_type = pulse::transport::spsc::spsc_queue<std::uint32_t, Capacity>;
    constexpr std::int64_t message_bytes {sizeof(std::uint32_t)};
    const auto batch_size = static_cast<std::size_t>(state.range(0));

    // Group batches to amortize Google Benchmark's iteration overhead.
    constexpr std::size_t target_messages_per_iteration {1024};
    const auto batches_per_iteration = std::max(std::size_t {1}, target_messages_per_iteration / batch_size);
    const auto messages_per_iteration = static_cast<std::int64_t>(batches_per_iteration * batch_size);
    const auto bytes_per_iteration = messages_per_iteration * message_bytes;
    if (state.max_iterations > std::numeric_limits<std::int64_t>::max() / bytes_per_iteration) {
        state.SkipWithError("The requested iteration count overflows the throughput counters");
        return;
    }

    auto queue = queue_type::create();
    if (!queue) {
        state.SkipWithError("Failed to create the SPSC queue");
        return;
    }
    auto producer = queue->try_attach_producer();
    auto consumer = queue->try_attach_consumer();
    if (!producer || !consumer) {
        state.SkipWithError("Failed to attach the SPSC endpoints");
        return;
    }

    const auto write_batches = [&](std::uint64_t batch_count) noexcept -> void {
        std::uint32_t sequence {0};
        const auto construct_segment = [&](const auto segment) noexcept -> void {
            for (std::size_t index = 0; index < segment.size(); ++index) {
                std::construct_at(segment[index], sequence++);
            }
        };
        for (std::uint64_t batch = 0; batch < batch_count; ++batch) {
            auto view = producer->try_reserve_up_to(batch_size);
            while (view.size() < batch_size) {
                view = producer->try_reserve_up_to(batch_size);
            }
            construct_segment(view.first());
            construct_segment(view.second());
            producer->commit(batch_size);
        }
    };

    const auto read_batch = [&](std::uint64_t & checksum) noexcept -> void {
        auto view = consumer->try_borrow_up_to(batch_size);
        while (view.size() < batch_size) {
            view = consumer->try_borrow_up_to(batch_size);
        }

        const auto sum_segment = [&](const auto segment) noexcept -> void {
            for (std::size_t index = 0; index < segment.size(); ++index) {
                checksum += segment[index];
            }
        };
        sum_segment(view.first());
        sum_segment(view.second());
        consumer->release(batch_size);
    };

    constexpr auto warmup_messages = std::max<std::uint64_t>(200'000, 2 * std::bit_ceil(Capacity));
    const auto warmup_batches = (warmup_messages + batch_size - 1) / batch_size;
    const auto measured_batches = static_cast<std::uint64_t>(state.max_iterations) * batches_per_iteration;

    std::latch producer_ready {1};
    std::latch start_measurement {1};
    bool producer_available {false};
    bool warmup_valid {false};
    std::jthread producer_thread([&] {
        producer_available = !cpus || scoped_cpu_affinity::pin_current_thread(cpus->first);
        producer_ready.count_down();
        if (!producer_available) {
            return;
        }

        write_batches(warmup_batches);
        start_measurement.wait();
        if (warmup_valid) {
            write_batches(measured_batches);
        }
    });

    producer_ready.wait();
    if (!producer_available) {
        state.SkipWithError("Failed to pin the producer thread");
        return;
    }

    // Drain the warmup before starting the timer and reuse the warmed queue and endpoints.
    std::uint64_t warmup_checksum {0};
    for (std::uint64_t batch = 0; batch < warmup_batches; ++batch) {
        read_batch(warmup_checksum);
    }
    warmup_valid = warmup_checksum == expected_checksum(warmup_batches * batch_size);
    if (!warmup_valid) {
        start_measurement.count_down();
        state.SkipWithError("The warmup checksum did not match");
        return;
    }

    std::uint64_t checksum {0};
    auto running = state.KeepRunning();
    start_measurement.count_down();
    for (; running; running = state.KeepRunning()) {
        for (std::size_t batch = 0; batch < batches_per_iteration; ++batch) {
            read_batch(checksum);
        }
    }
    producer_thread.join();

    const auto messages = state.iterations() * messages_per_iteration;
    if (checksum != expected_checksum(static_cast<std::uint64_t>(messages))) {
        state.SkipWithError("The measured payload checksum did not match");
        return;
    }
    state.SetItemsProcessed(messages);
    state.SetBytesProcessed(messages * message_bytes);
} catch (const std::bad_alloc &) {
    state.SkipWithError("Could not allocate storage for this throughput case");
} catch (const std::system_error & error) {
    state.SkipWithError(error.what());
}

template <std::size_t Capacity>
auto register_throughput(std::optional<cpu_pair> cpus) -> void
{
    const auto name = std::format("spsc_queue/throughput/uint32/capacity:{}/payload:{}", Capacity, sizeof(std::uint32_t));
    auto * test =
        benchmark::RegisterBenchmark(name, throughput<Capacity>, cpus)->ArgName("batch")->UseRealTime();

    constexpr std::array<std::int64_t, 11> batches {1, 4, 16, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
    for (const auto batch : batches) {
        if (std::cmp_greater(batch, Capacity)) {
            continue;
        }
        test->Arg(batch);
    }
}

auto register_cases(std::optional<cpu_pair> cpus) -> void
{
    register_throughput<64>(cpus);
    register_throughput<1024>(cpus);
    register_throughput<65536>(cpus);
    register_throughput<262144>(cpus);
    register_throughput<1048576>(cpus);
    register_throughput<2097152>(cpus);
    register_throughput<4194304>(cpus);
}

auto main(int argc, char * argv[]) -> int
{
    benchmark::MaybeReenterWithoutASLR(argc, argv);
    benchmark::Initialize(&argc, argv, print_help);

    const auto cpus = parse_args(argc, std::span<char *>(argv, argv + argc));
    if (!cpus && argc > 1) {
        std::fputs("No valid CPU pair was provided. Using system scheduling.\n", stderr);
    }
    register_cases(cpus);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
