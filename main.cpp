#include <iostream>
#include <array>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <cstdlib>
#include <numeric>
#include <algorithm>
#include <chrono>
#include <vector>
#include <thread>
#include <fstream>

// Globalne zmienne
const int ARRSIZE = 10000;
std::mutex global_mutex;
std::condition_variable cv_producer;
std::condition_variable cv_consumer;
std::vector<float> times;

// Kolejka FIFO
class Queue {
private:
    int max_capacity;
    int current_size;
    bool is_locked;
    std::unique_ptr<std::array<int, ARRSIZE>[]> buffer;
    int head, tail;

public:
    Queue(int capacity)
        : max_capacity(capacity), current_size(0), is_locked(true),
          buffer(new std::array<int, ARRSIZE>[capacity]), head(0), tail(0) {}

    int size() const {
        return current_size;
    }

    bool is_empty() const {
        return current_size == 0;
    }

    bool is_full() const {
        return current_size == max_capacity;
    }

    void set_lock_state(bool locked) {
        is_locked = locked;
    }

    bool locked() const {
        return is_locked;
    }

    void enqueue(const std::array<int, ARRSIZE>& element) {
        if (!is_full()) {
            buffer[tail] = element;
            tail = (tail + 1) % max_capacity;
            current_size++;
        }
    }

    void dequeue() {
        if (!is_empty()) {
            head = (head + 1) % max_capacity;
            current_size--;
        }
    }

    std::array<int, ARRSIZE> get_element(int index) const {
        if (index >= current_size) {
            return std::array<int, ARRSIZE>();
        } else if (!is_empty()) {
            int real_index = (head + index) % max_capacity;
            return buffer[real_index];
        }
        return std::array<int, ARRSIZE>();
    }
};

// Producent
class Producer {
private:
    int total_batches;
    Queue& target_buffer;

public:
    Producer(Queue& shared_queue, int batch_count)
        : total_batches(batch_count), target_buffer(shared_queue) {}

    void run_producer() {
        target_buffer.set_lock_state(true);
        std::array<int, ARRSIZE> random_array;

        for (int batch = 0; batch < total_batches; batch++) {
            for (int i = 0; i < ARRSIZE; i++) {
                random_array[i] = rand() % 256;
            }

            while (target_buffer.is_full()) {
                std::this_thread::yield();
            }

            std::unique_lock<std::mutex> guard(global_mutex);
            cv_producer.wait(guard, [&] { return !target_buffer.is_full(); });

            target_buffer.enqueue(random_array);

            guard.unlock();
            cv_consumer.notify_one();
        }

        target_buffer.set_lock_state(false);
        cv_consumer.notify_all();
    }
};

// Konsument
class Consumer {
private:
    Queue& shared_queue;
    int thread_id;

public:
    Consumer(Queue& q, int id) : shared_queue(q), thread_id(id) {}

    void run_consumer() {
        int processed = 0;
        std::array<int, ARRSIZE> buffer;
        auto t_start = std::chrono::high_resolution_clock::now();

        while (true) {
            while (shared_queue.is_empty() && shared_queue.locked()) {
                std::this_thread::yield();
            }

            std::unique_lock<std::mutex> guard(global_mutex);
            cv_consumer.wait(guard, [&] {
                return !shared_queue.is_empty() || !shared_queue.locked();
            });

            if (shared_queue.is_empty() && !shared_queue.locked()) {
                break;
            }

            buffer = shared_queue.get_element(0);
            if (buffer == std::array<int, ARRSIZE>()) {
                continue;
            }

            shared_queue.dequeue();
            guard.unlock();
            cv_producer.notify_one();

            std::sort(buffer.begin(), buffer.end());
            int checksum = std::accumulate(buffer.begin(), buffer.end(), 0);
            processed++;
        }

        auto t_end = std::chrono::high_resolution_clock::now();
        float duration = std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count() / 1000.0;

        std::unique_lock<std::mutex> lock(global_mutex);
        times.push_back(duration / processed);
        std::cout << "Consumer " << thread_id << " | sorted: " << processed
                  << " | time: " << duration << " ms | avg: " << duration / processed << " ms" << std::endl;
    }
};



int main() {
    unsigned int logical_processors = std::thread::hardware_concurrency();
    std::ofstream csv("results.csv");
    csv << "Multiplier,Queue capacity,Arrays,Consumer threads,Avg time per consumer,Avg time per element,Execution time\n";

    for (int multiplier = 1; multiplier <= 5; ++multiplier) {
        times.clear();

        int num_consumers = logical_processors * multiplier;
        int queue_capacity = num_consumers * 8;
        int arrays_to_produce = queue_capacity * 8;

        Queue queue(queue_capacity);
        Producer producer(queue, arrays_to_produce);

        std::vector<std::thread> consumer_threads;
        std::vector<std::unique_ptr<Consumer>> consumers;

        for (int i = 0; i < num_consumers; ++i) {
            consumers.push_back(std::make_unique<Consumer>(queue, i));
        }

        auto start = std::chrono::high_resolution_clock::now();

        std::thread producer_thread(&Producer::run_producer, &producer);

        for (int i = 0; i < num_consumers; ++i) {
            consumer_threads.emplace_back(&Consumer::run_consumer, consumers[i].get());
        }

        producer_thread.join();
        for (auto& t : consumer_threads) t.join();

        auto end = std::chrono::high_resolution_clock::now();
        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        float total_time = 0;
        for (float t : times) total_time += t;
        float avg_time = total_time / times.size();
        float avg_per_element = avg_time / 64.0f;

        csv << multiplier << ","
            << queue_capacity << ","
            << arrays_to_produce << ","
            << num_consumers << ","
            << avg_time << ","
            << avg_per_element << ","
            << total_ms << "\n";

        std::cout << "Done multiplier: " << multiplier << std::endl;
    }

    csv.close();
    std::cout << "Wyniki zapisane do results.csv" << std::endl;
    return 0;
}
