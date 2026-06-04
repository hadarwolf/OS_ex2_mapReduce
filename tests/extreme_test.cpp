//
// Created by yitzk on 6/4/2026.
//
#include "../MapReduceJob.h"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <cassert>
#include <random>

class HardKey : public K1, public K2, public K3 {
public:
    int k;
    explicit HardKey(int k) : k(k) {}
    bool operator<(const K1& other) const override { return k < static_cast<const HardKey&>(other).k; }
    bool operator<(const K2& other) const override { return k < static_cast<const HardKey&>(other).k; }
    bool operator<(const K3& other) const override { return k < static_cast<const HardKey&>(other).k; }
};

class HardVal : public V1, public V2, public V3 {
public:
    int v;
    explicit HardVal(int v) : v(v) {}
};

class ExtremeClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext& context) const override {
        // הכרחת מערכת ההפעלה להחליף הקשרים כדי לייצר תוהו ובוהו במקביליות
        if (rand() % 2 == 0) std::this_thread::yield();

        auto hk = std::static_pointer_cast<HardKey>(key);
        auto hv = std::static_pointer_cast<HardVal>(value);

        for (int i = 0; i < hv->v; ++i) {
            context.addIntermediate(std::make_shared<HardKey>(hk->k), std::make_shared<HardVal>(1));
        }
    }

    void reduce(const IntermediateVec& pairs, ReduceContext& context) const override {
        if (rand() % 2 == 0) std::this_thread::yield();
        if (pairs.empty()) return;

        auto hk = std::static_pointer_cast<HardKey>(pairs[0].first);
        int sum = 0;
        for (const auto& p : pairs) {
            sum += std::static_pointer_cast<HardVal>(p.second)->v;
        }
        context.addOutput(std::make_shared<HardKey>(hk->k), std::make_shared<HardVal>(sum));
    }
};

void testMoreThreadsThanInputs() {
    std::cout << "Test 1: Over-provisioning (50 Threads, 2 Inputs)... ";
    ExtremeClient client;
    InputVec input;
    input.push_back({std::make_shared<HardKey>(1), std::make_shared<HardVal>(1)});
    input.push_back({std::make_shared<HardKey>(2), std::make_shared<HardVal>(1)});

    MapReduceJob job(client, input, 50);
    job.wait();

    assert(job.getOutput().size() == 2 && "Output size must be exactly 2!");
    std::cout << "PASSED\n";
}

void testMassiveSingleKeyCollision() {
    std::cout << "Test 2: The Black Hole Key (10,000 identical keys)... ";
    ExtremeClient client;
    InputVec input;

    // 10,000 איברים שממופים כולם לאותו מפתח בדיוק
    for (int i = 0; i < 10000; i++) {
        input.push_back({std::make_shared<HardKey>(42), std::make_shared<HardVal>(2)});
    }

    MapReduceJob job(client, input, 10);
    job.wait();
    auto output = job.getOutput();

    assert(output.size() == 1 && "Shuffle failed to merge everything into one key!");
    assert(std::static_pointer_cast<HardVal>(output[0].second)->v == 20000 && "Data loss detected during massive reduce!");
    std::cout << "PASSED\n";
}

void testExtremeEmptyMix() {
    std::cout << "Test 3: Extreme Empty & Zero Values Mix... ";
    ExtremeClient client;
    InputVec input;

    // חצי מהערכים הם 0, מה שאומר שהלקוח ייצא מה-map מבלי לקרוא ל-addIntermediate בכלל
    for (int i = 0; i < 5000; i++) {
        input.push_back({std::make_shared<HardKey>(i % 5), std::make_shared<HardVal>(i % 2 == 0 ? 0 : 1)});
    }

    MapReduceJob job(client, input, 15);
    job.wait();
    auto output = job.getOutput();

    assert(job.isDone() && "Job is stuck and didn't finish nicely!");
    assert(output.size() == 5 && "Should have exactly 5 keys in the end.");
    std::cout << "PASSED\n";
}

int main() {
    srand(42); // שומרים על התנהגות דטרמיניסטית בתוך הכאוס
    std::cout << "--- STARTING EXTREME EDGE CASES TEST ---\n";
    testMoreThreadsThanInputs();
    testMassiveSingleKeyCollision();
    testExtremeEmptyMix();
    std::cout << "--- ALL EXTREME TESTS PASSED ---\n";
    return 0;
}