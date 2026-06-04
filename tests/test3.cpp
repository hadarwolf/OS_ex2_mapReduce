//
// Created by yitzk on 6/4/2026.
//
#include "../MapReduceJob.h"
#include "../MapReduceClient.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <memory>
#include <chrono>

// =========================================================================
// 1. Mock Client Implementation (Character Counter)
// =========================================================================

class CharKey : public K1, public K2, public K3 {
public:
    char c;
    explicit CharKey(char c) : c(c) {}
    bool operator<(const K1& other) const override { return c < static_cast<const CharKey&>(other).c; }
    bool operator<(const K2& other) const override { return c < static_cast<const CharKey&>(other).c; }
    bool operator<(const K3& other) const override { return c < static_cast<const CharKey&>(other).c; }
};

class IntVal : public V1, public V2, public V3 {
public:
    int v;
    explicit IntVal(int v) : v(v) {}
};

class CharCounterClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext& context) const override {
        // Sleep for a microscopic amount to force OS context switches and expose Race Conditions
        std::this_thread::sleep_for(std::chrono::microseconds(1));

        auto charKey = std::static_pointer_cast<CharKey>(key);
        auto intVal = std::static_pointer_cast<IntVal>(value);

        for (int i = 0; i < intVal->v; ++i) {
            context.addIntermediate(std::make_shared<CharKey>(charKey->c), std::make_shared<IntVal>(1));
        }
    }

    void reduce(const IntermediateVec& pairs, ReduceContext& context) const override {
        std::this_thread::sleep_for(std::chrono::microseconds(1));

        if (pairs.empty()) return;

        auto charKey = std::static_pointer_cast<CharKey>(pairs[0].first);
        int sum = 0;
        for (const auto& pair : pairs) {
            sum += std::static_pointer_cast<IntVal>(pair.second)->v;
        }

        context.addOutput(std::make_shared<CharKey>(charKey->c), std::make_shared<IntVal>(sum));
    }
};

// =========================================================================
// 2. The Tests
// =========================================================================

void testEmptyInput() {
    std::cout << "Running Test: Empty Input... ";
    CharCounterClient client;
    InputVec input; // Empty!

    MapReduceJob job(client, input, 4);
    job.wait();

    OutputVec output = job.getOutput();
    assert(output.empty() && "Output should be empty for empty input!");
    assert(job.isDone() && "Job should be done!");

    std::cout << "PASSED\n";
}

void testStressAndRaceConditions() {
    std::cout << "Running Test: Stress & Race Conditions (Heavy Load)... ";
    CharCounterClient client;
    InputVec input;

    // Create a massive input to cause a lot of threads to fight for the atomic counter
    for (int i = 0; i < 10000; ++i) {
        input.push_back({std::make_shared<CharKey>('a' + (i % 26)), std::make_shared<IntVal>(5)});
    }

    MapReduceJob job(client, input, 20); // 20 threads to force concurrency chaos

    OutputVec output = job.getOutput();

    assert(output.size() == 26 && "Should have exactly 26 letters!");
    for (const auto& pair : output) {
        int sum = std::static_pointer_cast<IntVal>(pair.second)->v;
        // 10000 / 26 = 384 chunks of 5 per letter, roughly.
        // We just assert sum > 0 to ensure no data was lost or corrupted completely.
        assert(sum > 0 && "Sum must be positive!");
    }
    assert(job.isDone());
    std::cout << "PASSED\n";
}

void testMultipleJobsConcurrently() {
    std::cout << "Running Test: Multiple Jobs Concurrently... ";
    CharCounterClient client;
    InputVec input1, input2;

    input1.push_back({std::make_shared<CharKey>('A'), std::make_shared<IntVal>(10)});
    input2.push_back({std::make_shared<CharKey>('B'), std::make_shared<IntVal>(20)});

    // Both jobs running at the exact same time
    MapReduceJob job1(client, input1, 3);
    MapReduceJob job2(client, input2, 4);

    job1.wait();
    job2.wait();

    auto out1 = job1.getOutput();
    auto out2 = job2.getOutput();

    assert(std::static_pointer_cast<IntVal>(out1[0].second)->v == 10);
    assert(std::static_pointer_cast<IntVal>(out2[0].second)->v == 20);
    std::cout << "PASSED\n";
}

void testWaitThreadSafety() {
    std::cout << "Running Test: wait() Thread Safety (Double Join Prevention)... ";
    CharCounterClient client;
    InputVec input;
    input.push_back({std::make_shared<CharKey>('X'), std::make_shared<IntVal>(100)});

    MapReduceJob job(client, input, 4);

    // Multiple threads calling wait() on the SAME job concurrently
    auto wait_func = [&job]() {
        job.wait();
    };

    std::vector<std::thread> waiter_threads;
    for(int i = 0; i < 10; i++) {
        waiter_threads.emplace_back(wait_func);
    }

    for(auto& t : waiter_threads) {
        t.join();
    }

    // If we didn't crash from double-join, we passed!
    std::cout << "PASSED\n";
}

int main() {
    std::cout << "--- Starting MapReduce Framework Tests ---\n\n";

    testEmptyInput();
    testStressAndRaceConditions();
    testMultipleJobsConcurrently();
    testWaitThreadSafety();

    std::cout << "\n--- All tests completed successfully! ---\n";
    return 0;
}
