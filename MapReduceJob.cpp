#include "MapReduceJob.h"


MapReduceJob::MapReduceJob(const MapReduceClient& client, const InputVec& inputVec, int multiThreadLevel):
    input_vec_(inputVec),
    intermediate_vectors(multiThreadLevel),
    client_(client),
    barrier_(multiThreadLevel),
    reduce_context_(output_vec_)
{
    for (int i = 0; i < multiThreadLevel; i++)
    {

        // init map_context
        map_context_.emplace_back(intermediate_vectors[i]);

        // set state of job number of jobs to left to do, and set up the state to map
        uint64_t jobs_left = inputVec.size() << 31;
        atomic_input_counter->store(add_one_to_map_state + jobs_left);

        // Create thread and add to the thread vec.
        threads.emplace_back(MapReduceJob::run_job, i, this);
    }
}

MapReduceState MapReduceJob::getState(void) const
{
    // TODO implement
}

void MapReduceJob::wait(void)
{
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

void MapReduceJob::map_phase(MapReduceJob* job)
{
    bool keep_getting_input_pairs = true;
    while (true)
    {
        uint64_t cur;
        uint64_t desired;
        do
        {
            // get the cur value and the value to be updated into(expected)
            cur = job->atomic_input_counter->load();
            desired = cur + add_one_to_num_proccessed - reduce_one_to_num_nonprocessed;

            // "(cur >> 31) & get_num_proccessed" is the bits that represent the number of processed pairs in input vec.
            if (((cur >> 31) & get_num_proccessed) <= 0)
            {
                keep_getting_input_pairs = false;
                break;
            }
        }
        while (!job->atomic_input_counter->compare_exchange_weak(cur, desired));

        // no more pairs
        if (!keep_getting_input_pairs) break;

        // get the actual pair to use the map method on
        int index_iput_pair = (cur >> 31) & get_num_proccessed; // TODO is this safe? i coverted from long to int.
        const InputPair& input_pair = job->input_vec_[index_iput_pair];

        // activate the map method.
        job->client_.map(input_pair.first, input_pair.second, job->map_context_[index_iput_pair]);
    }
}

void MapReduceJob::shuffle_phase(MapReduceJob* job)
{
    // indicates the index of the last vec in intermediate_vectors that is sequentualy empty from the front. (empty,empty(this),full,empty,full,full....)
    int last_non_empty_vec_index = 0;

    int num_intermidiate_vecs = job->intermediate_vectors.size(); // the number of vecs to shuffle
    while (last_non_empty_vec_index < num_intermidiate_vecs) // while there are vecs that are non empty in the intermidiate_vectors
    {
        for (int i = last_non_empty_vec_index; i < num_intermidiate_vecs; i++)
        {
            auto& cur_vec = job->intermediate_vectors[i];

            // optimisation to reduce unnecessary code execution.
            if (cur_vec.empty())
            {
                continue;
            }

            // if this is the first loop then we create the vector with key cur_vec[last_index].first
            if (i == last_non_empty_vec_index)
            {
                int last_index = cur_vec.size() - 1; //TODO is this safe to asighn a lomg to int?
                auto cur_key = cur_vec[last_index].first;

                // create the new vec.
                IntermediateVec new_vec = {IntermediatePair(cur_key, cur_vec[last_index].second)};

                // add the value of cur_key to the new_vec
                job->shuffeld_intermidiate_vecs.push_front(new_vec);

                // pop out the vec since we added it to the new vec in the shuffled vecs.
                cur_vec.pop_back();
            }

            // the key of the last vec added to shuffeled vecs.
            auto cur_key = job->shuffeld_intermidiate_vecs.front()[0].first;


            // keep adding values to the vec with key === cur_key, till there are no more elem with that key in cur_vec, or it is empty.
            auto& new_vec = job->shuffeld_intermidiate_vecs.front(); // the vec with the key == cur_key
            while (!cur_vec.empty() && cur_vec.front().first == cur_key)
            // while we didnt reach the end of the vec and we have that the next elem is
            {
                new_vec.emplace_back(cur_key, cur_vec.front().second);
                cur_vec.pop_back();
            }
             if(cur_vec.empty() && i == last_non_empty_vec_index) last_non_empty_vec_index++; // increment the index
        }
    }
    job->atomic_input_counter += add_one_to_map_state; // change state to reduce

}

void MapReduceJob::reduce_phase(MapReduceJob* job)
{
    // while there are more shuffled vectors to reduce
    while(!job->shuffeld_intermidiate_vecs.empty()){

        // lock the mutex so that only one thread at a time will pop the shuffeld_intermidiate_vecs queue.
        job->pop_queue_mutex.lock();

        IntermediateVec shuffled_vec; // the vec to be reduced
        if(!job->shuffeld_intermidiate_vecs.empty()) // doing a double check to make sure that maby more than one thread
                                                     //passed the first while loop with a context switch and then the
                                                     // second to pop can have undefined behavior(the queue could have emptied
        {
            // pop the vector to be reduced
            shuffled_vec = job->shuffeld_intermidiate_vecs.front();
            job->shuffeld_intermidiate_vecs.pop_front();

        }
        else  // race condition case
        {
            job->pop_queue_mutex.unlock();
            break;
        }

        job->pop_queue_mutex.unlock();

        job->client_.reduce(shuffled_vec, job->reduce_context_);

    }
}

void MapReduceJob::run_job(int thread_id, MapReduceJob* job)
{
    // map the input vec into the threads intermidiate vec

    MapReduceJob::map_phase(job);

    // sort the thread's intermidiate vec
    std::sort(job->intermediate_vectors[thread_id].begin(), job->intermediate_vectors[thread_id].end());

    // set up barrier here so all threads will finish sorting before the shuffle phase
    job->barrier_.arrive_and_wait();

    // shuffle the intermidiate vecs with thread 0
    if (thread_id == 0)
    {
        job->atomic_input_counter += add_one_to_map_state; // change state to shuffle
        MapReduceJob::shuffle_phase(job);
        //intermediate_vectors.erase(); //TODO should we release that vec now that we dont need it?
    }

    // set up barrier here so all threads will wait till thread 0 will finish the shuffle phase
     job->barrier_.arrive_and_wait();

    // have the thread reduce as many intermidiate vec as it can
    MapReduceJob::reduce_phase(job);
}


bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}

