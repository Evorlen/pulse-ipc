// SPDX-FileCopyrightText: 2026 Evorlen
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <thread>

#include <pulse/transport/spsc/spsc_queue.hpp>

constexpr std::size_t capacity {4096};
constexpr std::size_t message_count {100000};
constexpr std::size_t batch_size {64};

enum class Kind : std::uint8_t
{
    Data = 0,
    End = 1,
};

struct Message
{
    std::uint64_t sequence_id {0};
    Kind kind {Kind::Data};
    std::string message;
};

using queue_type = pulse::transport::spsc::spsc_queue<Message, capacity>;

struct aligned_byte_delete
{
    std::size_t alignment;

    auto operator()(std::byte * pointer) const noexcept -> void
    {
        ::operator delete(pointer, std::align_val_t {alignment});
    }
};

auto produce(queue_type & queue, std::atomic<int> & status) -> void
{
    auto producer = queue.try_attach_producer();
    if (!producer) [[unlikely]] {
        std::cerr << "Failed to attach producer: " << producer.error().encode() << '\n';
        status.store(1, std::memory_order_relaxed);
        return;
    }

    // Reserve raw slots, construct each message once, then publish the batch.
    // An empty view means the queue is full; the reservation starts only after a nonempty view.
    std::size_t sequence_id {0};
    while (sequence_id < message_count) {
        const auto remaining = message_count - sequence_id;
        auto write_view = producer->try_reserve_up_to(std::min(batch_size, remaining));
        if (write_view.empty()) {
            continue;
        }

        const auto construct_segment = [&](const auto segment) -> void {
            for (std::size_t index = 0; index < segment.size(); ++index) {
                const auto kind = sequence_id + 1 == message_count ? Kind::End : Kind::Data;
                std::construct_at(
                    segment[index],
                    sequence_id,
                    kind,
                    kind == Kind::End ? "end" : "pulse"
                );
                ++sequence_id;
            }
        };
        construct_segment(write_view.first());
        construct_segment(write_view.second());
        producer->commit(write_view.size());
    }

    std::cout << "Produced " << message_count << " messages\n";
}

auto consume(queue_type & queue, std::atomic<int> & status) -> void
{
    auto consumer = queue.try_attach_consumer();
    if (!consumer) [[unlikely]] {
        std::cerr << "Failed to attach consumer: " << consumer.error().encode() << '\n';
        status.store(1, std::memory_order_relaxed);
        return;
    }

    // Borrow committed messages, then release them so their slots can be reused.
    // An empty view means the producer has not published more messages yet.
    std::uint64_t next_sequence_id {0};
    bool sequence_error {false};
    bool saw_end {false};
    while (!sequence_error && !saw_end) {
        auto read_view = consumer->try_borrow_up_to(batch_size);
        if (read_view.empty()) {
            continue;
        }

        std::size_t accepted {0};
        const auto consume_segment = [&](const auto segment) -> void {
            for (std::size_t index = 0; index < segment.size() && !sequence_error && !saw_end; ++index) {
                const auto & message = segment[index];
                const auto expected = message.kind == Kind::End ? "end" : "pulse";
                if (message.sequence_id != next_sequence_id || message.message != expected) [[unlikely]] {
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
    }

    if (sequence_error) [[unlikely]] {
        std::cerr << "Unexpected message sequence\n";
        status.store(1, std::memory_order_relaxed);
        return;
    }
    if (next_sequence_id != message_count) [[unlikely]] {
        std::cerr << "Unexpected end message\n";
        status.store(1, std::memory_order_relaxed);
        return;
    }

    std::cout << "Consumed " << message_count << " messages\n";
}

auto main() -> int
{
    // The caller owns this allocation. Keep it alive until the queue has been destroyed.
    const auto bytes = queue_type::required_bytes();
    const auto alignment = queue_type::required_alignment();
    auto * raw = static_cast<std::byte *>(::operator new(bytes, std::align_val_t {alignment}));
    std::unique_ptr<std::byte, aligned_byte_delete> memory {raw, aligned_byte_delete {alignment}};

    // Construct the queue state in the caller-provided memory.
    auto queue = queue_type::create(std::span<std::byte> {memory.get(), bytes});
    if (!queue) [[unlikely]] {
        std::cerr << "Failed to create queue: " << queue.error().encode() << '\n';
        return 1;
    }

    // Join both threads before the queue is destroyed. jthread joins in its destructor,
    // and these objects are declared after the queue so they are destroyed first.
    // The memory owner is declared before the queue, so it is released afterward.
    std::atomic<int> producer_status {0};
    std::atomic<int> consumer_status {0};
    std::jthread producer_thread(produce, std::ref(*queue), std::ref(producer_status));
    std::jthread consumer_thread(consume, std::ref(*queue), std::ref(consumer_status));
    producer_thread.join();
    consumer_thread.join();

    if (producer_status.load(std::memory_order_relaxed) != 0 ||
        consumer_status.load(std::memory_order_relaxed) != 0) {
        return 1;
    }

    return 0;
}
