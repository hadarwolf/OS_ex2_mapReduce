#ifndef REDUCE_CONTEXT_H
#define REDUCE_CONTEXT_H

#include "MapReduceKeys.h"

class ReduceContext
{
public:
    /**
     * Constructor
     * @param output_vec The refrense to the data structure to add pairs to
     */
    explicit ReduceContext(OutputVec& output_vec): output_vec(output_vec){}

    /**
     *  Adds the listed vars(key and value) as a IntermediatePair object to a inner data structure safly( without race condition).
     * @param key The key pointer to be added to the data structure.
     * @param value The Value pointer to be added to the data structure.
     */
    void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);


private:
    OutputVec& output_vec;
    std::mutex output_vec_mutex;
};

#endif // REDUCE_CONTEXT_H