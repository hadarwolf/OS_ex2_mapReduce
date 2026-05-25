#ifndef MAP_CONTEXT_H
#define MAP_CONTEXT_H

#include "MapReduceKeys.h"


/**
 * The calss is responsible to hold the data of the intermidiate stage before the shuffle stage.
 */
class MapContext
{
public:
    /**
     * Constructor
     * @param intermediate_vec The refrense to the data structure to add pairs to
     */
    MapContext(IntermediateVec& intermediate_vec): intermidiate_vec(intermidiate_vec){}

    /**
     * Adds the listed vars(key and value) as a IntermediatePair object to a inner data structure.
     * @param key The key pointer to be added to the data structure.
     * @param value The Value pointer to be added to the data structure.
     */
    void addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value);



private:
    IntermediateVec& intermidiate_vec; // The chosen data structure
};

#endif // MAP_CONTEXT_H