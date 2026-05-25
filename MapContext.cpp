#include "MapContext.h"

// implement here your constructor and destructor

void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
    intermidiate_vec.push_back(IntermediatePair(key,value));
}
