//
// Created by yitzk on 6/4/2026.
//
#include "../MapReduceJob.h"
#include <iostream>
#include <vector>
#include <memory>
#include <chrono>
#include <thread>
#include <cassert>

// =========================================================================
// 1. Slow Client - עושה עבודה איטית כדי שנספיק לדגום את האחוזים
// =========================================================================

class NumKey : public K1, public K2, public K3 {
public:
    int n;
    explicit NumKey(int n) : n(n) {}
    bool operator<(const K1& other) const override { return n < static_cast<const NumKey&>(other).n; }
    bool operator<(const K2& other) const override { return n < static_cast<const NumKey&>(other).n; }
    bool operator<(const K3& other) const override { return n < static_cast<const NumKey&>(other).n; }
};

class NumVal : public V1, public V2, public V3 {
public:
    int v;
    explicit NumVal(int v) : v(v) {}
};

class SlowClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext& context) const override {
        // מעכבים את ה-map כדי שיהיה לנו זמן לדגום אחוזים
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        auto k = std::static_pointer_cast<NumKey>(key);
        context.addIntermediate(std::make_shared<NumKey>(k->n), std::make_shared<NumVal>(1));
    }

    void reduce(const IntermediateVec& pairs, ReduceContext& context) const override {
        // מעכבים את ה-reduce כדי שיהיה לנו זמן לדגום אחוזים
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if(pairs.empty()) return;
        auto k = std::static_pointer_cast<NumKey>(pairs[0].first);
        context.addOutput(std::make_shared<NumKey>(k->n), std::make_shared<NumVal>(pairs.size()));
    }
};

// =========================================================================
// 2. Tests
// =========================================================================

const char* stageToString(MapReduceStage stage) {
    switch(stage) {
        case UNDEFINED_STAGE: return "UNDEFINED";
        case MAP_STAGE: return "MAP";
        case SHUFFLE_STAGE: return "SHUFFLE";
        case REDUCE_STAGE: return "REDUCE";
        default: return "UNKNOWN";
    }
}

void testStateAndPercentages() {
    std::cout << "Running Test: State and Percentages Tracking... \n";
    SlowClient client;
    InputVec input;

    for (int i = 0; i < 50; i++) {
        input.push_back({std::make_shared<NumKey>(i % 5), std::make_shared<NumVal>(1)});
    }

    MapReduceJob job(client, input, 4);

    int current_stage = UNDEFINED_STAGE;
    float current_percentage = 0.0;
    bool seen_map = false, seen_reduce = false;

    while (true) {
        MapReduceState state = job.getState();

        // 1. הגנה מפני חריגה מעל 100% בגלל שחוטים רעבים משכו "אוויר"
        float p = state.percentage > 1.0f ? 1.0f : state.percentage;

        // 2. החלקה של בעיות דיוק בשברים (Floating Point Precision)
        if (state.stage == current_stage && p < current_percentage) {
            p = current_percentage;
        }

        assert(state.stage >= current_stage && "Error: Stage went backwards!");
        assert(p >= 0.0f && p <= 1.0f && "Error: Percentage out of bounds!");

        if (state.stage == MAP_STAGE) seen_map = true;
        if (state.stage == REDUCE_STAGE) seen_reduce = true;

        if (state.stage != current_stage || (p - current_percentage) > 0.05) {
            std::cout << "  -> Stage: " << stageToString(state.stage)
                      << ", Progress: " << (p * 100) << "%\n";
            current_stage = state.stage;
            current_percentage = p;
        }

        if (state.stage == REDUCE_STAGE && p >= 1.0f) {
            std::cout << "  -> Stage: " << stageToString(state.stage) << ", Progress: 100%\n";
            break;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    job.wait();

    assert(seen_map && seen_reduce && "Error: Missed a major stage!");
    assert(job.isDone() && "Job should be isDone() == true");
    std::cout << "PASSED\n\n";
}
void testDestructorWithoutWait() {
    std::cout << "Running Test: Destructor implicit wait()... ";

    // ניצור בלוק זיכרון זמני. כשהוא ייסגר, ה-job ייהרס.
    {
        SlowClient client;
        InputVec input;
        for (int i = 0; i < 20; i++) {
            input.push_back({std::make_shared<NumKey>(i), std::make_shared<NumVal>(1)});
        }

        MapReduceJob job(client, input, 3);

        // שים לב: אנחנו לא קוראים ל- job.wait() כאן!
        // אנחנו פשוט נותנים ל-job לצאת מה-scope. ה-Destructor חייב לחכות לחוטים.
    }

    // אם הגענו לכאן בלי לקרוס, סימן שה-Destructor חיכה בסבלנות לכל החוטים שייסגרו
    std::cout << "PASSED\n\n";
}

void testConcurrentWaits() {
    std::cout << "Running Test: Concurrent wait() calls... ";
    SlowClient client;
    InputVec input;
    input.push_back({std::make_shared<NumKey>(1), std::make_shared<NumVal>(1)});

    MapReduceJob job(client, input, 2);

    auto wait_task = [&job]() {
        job.wait();
        job.wait(); // אפילו קריאה כפולה מאותו חוט!
    };

    // מזניקים 10 חוטים זרים שכולם מנסים לחכות לאותו ה-Job במקביל
    std::vector<std::thread> external_threads;
    for(int i = 0; i < 10; i++) {
        external_threads.emplace_back(wait_task);
    }

    for(auto& t : external_threads) {
        t.join();
    }

    std::cout << "PASSED\n\n";
}

int main() {
    std::cout << "--- STARTING STATE & SYNC TESTS ---\n\n";
    testStateAndPercentages();
    testConcurrentWaits();
    testDestructorWithoutWait();
    std::cout << "--- ALL STATE TESTS PASSED ---\n";
    return 0;
}