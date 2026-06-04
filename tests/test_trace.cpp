//
// Created by yitzk on 6/4/2026.
//
#include "../MapReduceJob.h"
#include <iostream>
#include <memory>

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

class TraceClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext& context) const override {
        auto k = std::static_pointer_cast<CharKey>(key);
        printf("[Client] Map called for key: %c\n", k->c);
        context.addIntermediate(std::make_shared<CharKey>(k->c), std::make_shared<IntVal>(1));
    }

    void reduce(const IntermediateVec& pairs, ReduceContext& context) const override {
        if(pairs.empty()) return;
        auto k = std::static_pointer_cast<CharKey>(pairs[0].first);
        printf("[Client] Reduce called for key: %c with %zu pairs\n", k->c, pairs.size());
        context.addOutput(std::make_shared<CharKey>(k->c), std::make_shared<IntVal>(pairs.size()));
    }
};

int main() {
    printf("--- STARTING TRACE TEST ---\n");
    TraceClient client;
    InputVec input;

    // ניצור קלט בכוונה שמייצר מפתחות זהים כדי לאלץ את לולאת ה-Shuffle לעבוד קשה
    input.push_back({std::make_shared<CharKey>('A'), std::make_shared<IntVal>(1)});
    input.push_back({std::make_shared<CharKey>('B'), std::make_shared<IntVal>(1)});
    input.push_back({std::make_shared<CharKey>('A'), std::make_shared<IntVal>(1)});
    input.push_back({std::make_shared<CharKey>('C'), std::make_shared<IntVal>(1)});

    printf("[Main] Creating Job with 2 threads...\n");
    MapReduceJob job(client, input, 2);

    printf("[Main] Waiting for Job...\n");
    job.wait();

    printf("[Main] Job Done! Calling getOutput...\n");
    auto out = job.getOutput();

    printf("--- FINAL OUTPUT ---\n");
    for(auto& p : out) {
        printf("(%c, %d)\n", std::static_pointer_cast<CharKey>(p.first)->c, std::static_pointer_cast<IntVal>(p.second)->v);
    }

    return 0;
}
