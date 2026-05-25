#include "ReduceContext.h"

#include <mutex>

// implement here your constructor and destructor

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    std::unique_lock<std::mutex> lock(output_vec_mutex);
    output_vec.push_back(OutputPair(key, value));
}