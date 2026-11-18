// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#include <pulse/channel/spsc/spsc_channel.hpp>
#include <pulse/memory/shared_memory.hpp>
#include <pulse/transport/spsc/spsc_consumer.hpp>
#include <pulse/transport/spsc/spsc_producer.hpp>

constexpr std::size_t capacity {4096};
constexpr std::uint32_t schema_id {1};
constexpr std::size_t message_count {100000};
constexpr std::size_t batch_size {64};
constexpr std::chrono::seconds idle_timeout {30};

enum class Kind : std::uint8_t
{
    Data = 0,
    End = 1,
};

struct Message
{
    std::uint64_t sequence_id {0};
    Kind kind {Kind::Data};
    std::uint32_t size {0};
    std::array<std::byte, 128> data {std::byte {0}};
};

auto print_usage(const char * program_name) -> void
{
    std::cout << "Usage: " << program_name << " [producer | consumer | cleanup] <channel_name>" << '\n';
    std::cout << "Actions:" << '\n';
    std::cout << "  producer: produce messages to the channel" << '\n';
    std::cout << "  consumer: consume messages from the channel" << '\n';
    std::cout << "  cleanup: cleanup the channel" << '\n';
    std::cout << "Channel name: an unique name for the channel, must start with a '/' character" << '\n';
    std::cout << "  Example: /my_channel" << '\n';
    std::cout << "Transfers time out after " << idle_timeout.count() << " seconds without progress" << '\n';
}

auto main(int argc, char * argv[]) -> int
{
    if (argc != 3) [[unlikely]] {
        std::cerr << "Invalid arguments number" << '\n';
        print_usage(argv[0]);
        return 1;
    }

    const auto action = argv[1];
    if (std::strcmp(action, "producer") != 0 && std::strcmp(action, "consumer") != 0 &&
        std::strcmp(action, "cleanup") != 0) [[unlikely]] {
        std::cerr << "Invalid action: " << action << '\n';
        print_usage(argv[0]);
        return 1;
    }

    const auto channel_name = argv[2];
    if (std::strlen(channel_name) <= 1 || channel_name[0] != '/') [[unlikely]] {
        std::cerr << "Invalid channel name: " << channel_name << '\n';
        print_usage(argv[0]);
        return 1;
    }

    if (std::strcmp(action, "producer") == 0) {
        // Create a named channel.
        auto channel = pulse::channel::spsc::spsc_channel<Message, capacity>::create(channel_name, schema_id);
        if (!channel) [[unlikely]] {
            std::cerr << "Failed to create channel: " << channel.error().encode() << '\n';
            return 1;
        }

        // Try to attach a producer to the channel.
        auto producer = channel->try_attach_producer();
        if (!producer) [[unlikely]] {
            std::cerr << "Failed to attach producer: " << producer.error().encode() << '\n';
            return 1;
        }

        // Reserve raw slots, construct each message once, then publish the batch.
        // An empty view means the queue is full; the reservation starts only after a nonempty view.
        std::size_t sequence_id {0};
        auto last_progress = std::chrono::steady_clock::now();
        while (sequence_id < message_count) {
            const auto remaining = message_count - sequence_id;
            auto write_view = producer->try_reserve_up_to(std::min(batch_size, remaining));
            if (write_view.empty()) {
                if (std::chrono::steady_clock::now() - last_progress >= idle_timeout) [[unlikely]] {
                    std::cerr << "Producer timed out after " << idle_timeout.count()
                              << " seconds without publishing messages\n";
                    return 1;
                }
                continue;
            }

            const auto construct_segment = [&](const auto segment) -> void {
                for (std::size_t index = 0; index < segment.size(); ++index) {
                    const auto kind = sequence_id + 1 == message_count ? Kind::End : Kind::Data;
                    std::construct_at(segment[index], Message {.sequence_id = sequence_id, .kind = kind});
                    ++sequence_id;
                }
            };
            construct_segment(write_view.first());
            construct_segment(write_view.second());
            producer->commit(write_view.size());
            last_progress = std::chrono::steady_clock::now();
        }

        std::cout << "Produced " << message_count << " messages\n";
    } else if (std::strcmp(action, "consumer") == 0) {
        // Open the channel.
        auto channel = pulse::channel::spsc::spsc_channel<Message, capacity>::open(channel_name, schema_id);
        if (!channel) [[unlikely]] {
            std::cerr << "Failed to open channel: " << channel.error().encode() << '\n';
            return 1;
        }

        // Try to attach a consumer to the channel.
        auto consumer = channel->try_attach_consumer();
        if (!consumer) [[unlikely]] {
            std::cerr << "Failed to attach consumer: " << consumer.error().encode() << '\n';
            return 1;
        }

        // Borrow committed messages, then release them so their slots can be reused.
        // An empty view means the producer has not published more messages yet.
        std::uint64_t next_sequence_id {0};
        bool sequence_error {false};
        bool saw_end {false};
        auto last_progress = std::chrono::steady_clock::now();
        while (!sequence_error && !saw_end) {
            auto read_view = consumer->try_borrow_up_to(batch_size);
            if (read_view.empty()) {
                if (std::chrono::steady_clock::now() - last_progress >= idle_timeout) [[unlikely]] {
                    std::cerr << "Consumer timed out after " << idle_timeout.count()
                              << " seconds without consuming messages\n";
                    return 1;
                }
                continue;
            }

            std::size_t accepted {0};
            const auto consume_segment = [&](const auto segment) -> void {
                for (std::size_t index = 0; index < segment.size() && !sequence_error && !saw_end; ++index) {
                    const auto & message = segment[index];
                    if (message.sequence_id != next_sequence_id) [[unlikely]] {
                        sequence_error = true;
                        return;
                    }
                    ++next_sequence_id;
                    ++accepted;
                    saw_end = message.kind == Kind::End;
                }
            };
            consume_segment(read_view.first());
            consume_segment(read_view.second());
            consumer->release(accepted);
            if (accepted != 0) {
                last_progress = std::chrono::steady_clock::now();
            }
        }

        if (sequence_error) [[unlikely]] {
            std::cerr << "Unexpected message sequence\n";
            return 1;
        }
        if (next_sequence_id != message_count) [[unlikely]] {
            std::cerr << "Unexpected end message\n";
            return 1;
        }

        std::cout << "Consumed " << message_count << " messages\n";
    } else if (std::strcmp(action, "cleanup") == 0) {
        // Remove the name. Mappings already held by a producer or consumer stay valid until they exit.
        auto unlinked = pulse::memory::shared_memory::unlink(channel_name);
        if (!unlinked) [[unlikely]] {
            std::cerr << "Failed to unlink channel: " << unlinked.error().encode() << '\n';
            return 1;
        }

        std::cout << "Channel: " << channel_name << " cleaned up\n";
    }

    return 0;
}
