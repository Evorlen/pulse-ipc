// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <latch>
#include <new>
#include <optional>
#include <print>
#include <span>
#include <string_view>
#include <system_error>
#include <thread>

#if defined(__linux__)
#include <sched.h>
#else
#error "Unsupported platform"
#endif

#include <benchmark/benchmark.h>
#include <pulse/transport/spsc/spsc_queue.hpp>

namespace
{

constexpr std::size_t queue_capacity {64};

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
        "Usage: spsc_queue_rtt [options]\n"
        "Options:\n"
        "  --cpus=<initiator>,<responder> Bind the threads to two distinct Linux logical CPUs.\n"
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
        std::fputs("Invalid initiator CPU ID.\n", stderr);
        return std::nullopt;
    }
    const auto second = parse_cpu_id(value.substr(separator + 1));
    if (!second) {
        std::fputs("Invalid responder CPU ID.\n", stderr);
        return std::nullopt;
    }
    if (*first == *second) {
        std::fputs("Initiator and responder CPU IDs must be distinct.\n", stderr);
        return std::nullopt;
    }

    return cpu_pair {.first = *first, .second = *second};
}

template <std::size_t MessageBytes>
auto rtt(benchmark::State & state, std::optional<cpu_pair> cpus) -> void
try {
    std::optional<scoped_cpu_affinity> initiator_affinity;
    if (cpus) {
        initiator_affinity.emplace(cpus->first);
        if (!initiator_affinity->pinned()) {
            state.SkipWithError("Failed to pin the initiator thread");
            return;
        }
    }
    state.SetLabel(cpus ? std::format("cpu {} <-> {}", cpus->first, cpus->second) : "system scheduling");

    constexpr std::uint64_t warmup_round_trips {1024};
    constexpr std::byte reply_mask {0xff};
    using message_type = std::array<std::byte, MessageBytes>;
    using queue_type = pulse::transport::spsc::spsc_queue<message_type, queue_capacity>;
    static_assert(sizeof(message_type) == MessageBytes);

    auto request = queue_type::create();
    auto response = queue_type::create();
    if (!request || !response) {
        state.SkipWithError("Failed to create the RTT queues");
        return;
    }
    auto request_producer = request->try_attach_producer();
    auto request_consumer = request->try_attach_consumer();
    auto response_producer = response->try_attach_producer();
    auto response_consumer = response->try_attach_consumer();
    if (!request_producer || !request_consumer || !response_producer || !response_consumer) {
        state.SkipWithError("Failed to attach the RTT endpoints");
        return;
    }

    const auto send = [](auto & producer, std::byte value) noexcept -> void {
        auto * slot = producer.try_reserve();
        while (slot == nullptr) {
            slot = producer.try_reserve();
        }
        // Start the lifetime in the slot and write the full payload once.
        auto * message = ::new (static_cast<void *>(slot)) message_type;
        message->fill(value);
        producer.commit();
    };
    const auto receive = [](auto & consumer) noexcept -> std::byte {
        auto * message = consumer.try_borrow();
        while (message == nullptr) {
            message = consumer.try_borrow();
        }
        const auto value = message->front();
        consumer.release();
        return value;
    };

    const auto total_round_trips = warmup_round_trips + static_cast<std::uint64_t>(state.max_iterations);
    std::latch responder_ready {1};
    bool responder_available {false};
    std::jthread responder([&] {
        responder_available = !cpus || scoped_cpu_affinity::pin_current_thread(cpus->second);
        responder_ready.count_down();
        if (!responder_available) {
            return;
        }

        for (std::uint64_t round = 0; round < total_round_trips; ++round) {
            const auto value = receive(*request_consumer);
            send(*response_producer, value ^ reply_mask);
        }
    });
    responder_ready.wait();
    if (!responder_available) {
        state.SkipWithError("Failed to pin the responder thread");
        return;
    }

    std::uint8_t sequence {0};
    const auto round_trip = [&]() noexcept -> bool {
        const auto value = static_cast<std::byte>(sequence++);
        send(*request_producer, value);
        return receive(*response_consumer) == (value ^ reply_mask);
    };

    bool replies_valid {true};
    for (std::uint64_t round = 0; round < warmup_round_trips; ++round) {
        replies_valid &= round_trip();
    }
    for (auto _ : state) {
        replies_valid &= round_trip();
    }
    responder.join();

    if (!replies_valid) {
        state.SkipWithError("The RTT reply payload did not match the request");
    }
} catch (const std::bad_alloc &) {
    state.SkipWithError("Could not allocate storage for this RTT case");
} catch (const std::system_error & error) {
    state.SkipWithError(error.what());
}

template <std::size_t MessageBytes>
auto register_rtt(std::optional<cpu_pair> cpus) -> void
{
    const auto name =
        std::format("spsc_queue/rtt/zero_copy/capacity:{}/payload:{}", queue_capacity, MessageBytes);
    benchmark::RegisterBenchmark(name, rtt<MessageBytes>, cpus)->Unit(benchmark::kNanosecond)->UseRealTime();
}

auto register_cases(std::optional<cpu_pair> cpus) -> void
{
    register_rtt<8>(cpus);
    register_rtt<64>(cpus);
    register_rtt<1024>(cpus);
}

} // namespace

auto main(int argc, char * argv[]) -> int
{
    benchmark::MaybeReenterWithoutASLR(argc, argv);
    benchmark::Initialize(&argc, argv, print_help);

    const auto cpus = parse_args(argc, std::span<char *>(argv, argv + argc));
    if (!cpus && argc > 1) {
        benchmark::Shutdown();
        return 1;
    }
    register_cases(cpus);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
