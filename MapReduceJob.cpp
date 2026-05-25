#include "MapReduceJob.h"

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
{
    for(int i; i < multiThreadLevel; i++)
    {

    }
}

MapReduceState MapReduceJob::getState(void) const
{
    return job_state_;
}

void MapReduceJob::wait(void)
{
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
