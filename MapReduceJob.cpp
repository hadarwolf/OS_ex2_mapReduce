#include "MapReduceJob.h"


MapReduceJob::MapReduceJob(const MapReduceClient& client, const InputVec& inputVec, int multiThreadLevel):
    atomic_input_counter(UNDEFINED_STATE),
    input_vec_(inputVec),
    intermediate_vectors(multiThreadLevel),
    client_(client),
    barrier_(multiThreadLevel),
    reduce_context_(output_vec_)

{
    // reserve the size of the vector to prevent realocation of the vector when we add elemnts to it that can create a segfault.
    threads.reserve(multiThreadLevel);

    // get number of jobs to be done and shift it the right spot in the 64 bit counter
    uint64_t total_jobs = static_cast<uint64_t>(inputVec.size()) << 31;
    // set up the state to map with the number of total jobs
    atomic_input_counter.store(MAP_STATE + total_jobs);

    for (int i = 0; i < multiThreadLevel; i++)
    {
        // Create thread and add to the thread vec.
        threads.emplace_back(MapReduceJob::run_job, i, this);
    }
}

MapReduceJob::~MapReduceJob()
{
    this->wait();
}

MapReduceState MapReduceJob::getState(void) const
{
    // get the stage, number of pairs to process, and number of pairs already processed  from the atomic var.
    uint64_t data =  atomic_input_counter.load();
    int num_stage = data >> 62;
    int num_pairs_proccessed = data & get_num_pairs;
    int overall_pairs = (data >> 31) & get_num_pairs;

    // init percentage to 0(the undefined case).
    double percentage = 0;

    // Prevents division by 0.
    if (overall_pairs != 0 && num_stage != 0)
    {
        percentage = (double)num_pairs_proccessed / (double)(overall_pairs);
    }
    // Else we have 0 pairs to process so we are at 100% progression.
    else if(num_stage != 0) percentage = 1;

    if (percentage > 1.0) {
        percentage = 1.0;
    }

    //create the state with the required config
    MapReduceState state = MapReduceState();
    MapReduceStage stage = static_cast<MapReduceStage>(num_stage);
    state.percentage = percentage;
    state.stage = stage;

    return state;
}

bool MapReduceJob::isDone(void) const
{
    MapReduceState state = this->getState();
    return (state.stage == REDUCE_STAGE && state.percentage >= 1.0);
}

void MapReduceJob::wait(void)
{
    // lock the mutex so only the thirst thread to call this will be able to implement the logic of the function
    std::unique_lock<std::mutex> lock(wait_mutex);

    // makes sure that wait is not called twice(if called twice we will join threads more than once causing a crash).
    if(wait_wasnt_called)
    {
        wait_wasnt_called = false;
        for(auto& thread: threads)
        {
            thread.join();
        }
    }
}

OutputVec MapReduceJob::getOutput(void)
{
    // if the job is not done,then we will wait till it finishes and then deal with the output.
    this->wait();

    // by this stage the job is done so we only need to sort the output
    std::sort(output_vec_.begin(), output_vec_.end(), [](const OutputPair& pair_a,const OutputPair& pair_b)
    {
         return *pair_a.first < *pair_b.first;
    });

    return output_vec_;
}

void MapReduceJob::map_phase(MapReduceJob* job, int thread_id)
{
    MapContext local_context(job->intermediate_vectors[thread_id]);
    while (true)
    {
        // get the current state and number of processed pairs
        uint64_t cur = job->atomic_input_counter.fetch_add(1);

        int remaining_pairs = ((cur >> 31) & get_num_pairs) - (cur & get_num_pairs);

        // no more pairs
        if (remaining_pairs <= 0) break;

        // get the actual pair to use the map method on
        int index_iput_pair = cur & get_num_pairs;
        const InputPair& input_pair = job->input_vec_[index_iput_pair];

        // activate the map method.
        job->client_.map(input_pair.first, input_pair.second, local_context);
    }
}

void MapReduceJob::shuffle_phase(MapReduceJob* job)
{
    // indicates the index of the last vec in intermediate_vectors that is sequentualy empty from the front. example: (empty,empty(this),full,empty,full,full....)
    int last_non_empty_vec_index = 0;

    int num_intermidiate_vecs = job->intermediate_vectors.size(); // the number of vecs to shuffle

    // while there are vecs that are non empty in the intermidiate_vectors
    while (last_non_empty_vec_index < num_intermidiate_vecs)
    {

        if (job->intermediate_vectors[last_non_empty_vec_index].empty())
        {
            last_non_empty_vec_index++;
            continue;
        }
        // we create the vector with key that equal to the max val at the back of all the vecs that are not empty
        int max_vec_index = MapReduceJob::find_max_key_pair(job, last_non_empty_vec_index);
        auto key = job->intermediate_vectors[max_vec_index].back().first;
        auto val = job->intermediate_vectors[max_vec_index].back().second;


        //  create a new empty vec in shuffeld_intermidiate_vecs
        job->shuffeld_intermidiate_vecs.emplace_front();

        // get a refrence to the new vec and add the new pair with the key.
        IntermediateVec& new_vec = job->shuffeld_intermidiate_vecs.front();
        new_vec.emplace_back(key, val);

        // pop out the pair since we added it to the new vec in the shuffled vecs.
        job->intermediate_vectors[max_vec_index].pop_back();

        // now we go over all the remaining non empty vectors and look for values with key that is equal to the key above.
        for (int i = last_non_empty_vec_index; i < num_intermidiate_vecs; i++)
        {
            // the current vec to seach for key that is equal to the key above.
            auto& cur_vec = job->intermediate_vectors[i];

            // the for loop started with an empty vector.
            if (cur_vec.empty() && i == last_non_empty_vec_index)
            {
                last_non_empty_vec_index++;
                continue;
            }

            // the key of the last vec added to shuffeled vecs.
            auto cur_key = new_vec.front().first;


            // keep adding values to the vec with key === cur_key, till there are no more elem with that key in cur_vec, or it is empty.
            while (!cur_vec.empty() && !(*(cur_vec.back().first) < (*cur_key)) && !((*cur_key) < (*cur_vec.back().
                first)))
            // while we didnt reach the end of the vec and we have that the next elem is
            {
                new_vec.emplace_back(cur_key, cur_vec.back().second);
                cur_vec.pop_back();
            }
        }

        // add the number of pairs proccesed to the counter
        job->atomic_input_counter.fetch_add(new_vec.size());
    }
}

void MapReduceJob::reduce_phase(MapReduceJob* job)
{
    // while there are more shuffled vectors to reduce
    while (true)
    {
        // lock the mutex so that only one thread at a time will pop the shuffeld_intermidiate_vecs queue.
        job->pop_queue_mutex.lock();

        IntermediateVec shuffled_vec; // the vec to be reduced

        // if there are no vectors left in queue then unlock mutex and finish the reduce with break.
        if (job->shuffeld_intermidiate_vecs.empty())
        {
            job->pop_queue_mutex.unlock();
            break;
        }

        // else we can pop a vec to reduce it.
        shuffled_vec = std::move(job->shuffeld_intermidiate_vecs.front());
        job->shuffeld_intermidiate_vecs.pop_front();

        job->pop_queue_mutex.unlock();

        // reduce the vec
        job->client_.reduce(shuffled_vec, job->reduce_context_);

        // update counter
        job->atomic_input_counter.fetch_add(shuffled_vec.size());
    }
}

void MapReduceJob::run_job(int thread_id, MapReduceJob* job)
{
    map_phase(job, thread_id);

    std::sort(job->intermediate_vectors[thread_id].begin(), job->intermediate_vectors[thread_id].end(),
        [](const IntermediatePair& pair_a,const IntermediatePair& pair_b)
        {
            return *pair_a.first < *pair_b.first;
        });

    job->barrier_.arrive_and_wait();

    if (thread_id == 0)
    {
        job->calculate_num_intermidiate_pairs();
        job->atomic_input_counter.store(SHUFFLE_STATE + (job->num_intermidiate_pairs << 31));

        shuffle_phase(job);

        job->atomic_input_counter.store(REDUCE_STATE + (job->num_intermidiate_pairs << 31));
    }

    job->barrier_.arrive_and_wait();

    reduce_phase(job);
}

void MapReduceJob::calculate_num_intermidiate_pairs()
{
    int num_intermidiate_vec = intermediate_vectors.size();
    num_intermidiate_pairs = 0;
    for (int i = 0; i < num_intermidiate_vec; i++)
    {
        num_intermidiate_pairs += intermediate_vectors[i].size();
    }
}

int MapReduceJob::find_max_key_pair(MapReduceJob* job, int last_non_empty_vec_index)
{
    int num_intermidiate_vecs = job->intermediate_vectors.size();
    int max_index = last_non_empty_vec_index;
    auto max_key = job->intermediate_vectors[last_non_empty_vec_index].back().first;
    for (int i = last_non_empty_vec_index + 1; i < num_intermidiate_vecs; i++)
    {
        if (!job->intermediate_vectors[i].empty() && *max_key < *(job->intermediate_vectors[i].back().first))
        {
            max_index = i;
            max_key = job->intermediate_vectors[i].back().first;
        }
    }
    return max_index;
}