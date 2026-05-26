#include "MapReduceJob.h"
#include <math.h>

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel):
input_vec_(inputVec),
//TODO init jobState var
threads(new std::thread[multiThreadLevel]),
intermediate_vectors(new IntermediateVec[multiThreadLevel])
{

    for(int i; i < multiThreadLevel; i++)
    {
        // set state of job number of jobs to left to do
        uint64_t jobs_left = inputVec.size()<<31;
        uint64_t state = 1<<62;
        atomic_input_counter->store(state + jobs_left);

        // Create thread and add to the thread vec.
        threads[i] = std::thread(MapReduceJob::run_job, i,this);
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
