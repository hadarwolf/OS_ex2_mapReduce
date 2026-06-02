#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H


#include "MapReduceClient.h"
#include <thread>
#include <barrier>
#include<algorithm>
#include <list>
#include <map>

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	/**
	 * Operator == overload.
	 * @param other the other state Object
	 * @return the answer is it equal
	 */
	inline bool operator==(const MapReduceState &other) const
	{
		// They are equal if they are in the same state and their progression precent is at most 0.000001 away from each other.
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
	friend class std::thread; // allow threads to have access to private fields and methods.

	const MapReduceClient& client_; // the client with the map and reduce methods
	std::vector<std::thread> threads;
	std::vector<IntermediateVec> intermediate_vectors;
	std::list<IntermediateVec> shuffeld_intermidiate_vecs; // The intermidiate vectors after the shuffle phase.

	std::mutex pop_queue_mutex;
	ReduceContext reduce_context_; // deals with the

	bool changed_state = false;

	std::vector<MapContext> map_context_; // holds and manages the intermidite pairs of each thread, each thread gets 1 map context.
	OutputVec output_vec_;
	const InputVec& input_vec_;
	std::atomic<uint64_t> atomic_input_counter; // will hold in first 2 bits the state ofnthe job,
	// next 31 bit are the number of pairs proccessed in input_vec,
	//next 31 bits are the number of paires left to proccess in input_vec.
	static const uint64_t get_num_proccessed = 0x7fffffff;  // 7 in hex is 0011 in binary,
	static  const uint64_t add_one_to_num_proccessed = 1;
	static const uint64_t add_one_to_map_state = 0x4000000000000000;
	static const uint64_t reduce_one_to_num_nonprocessed = 0x80000000;

	std::barrier<> barrier_;

	/**
	 *
	 * @param thread_id the thread id that starts in this function
	 * @param job the current job.
	 */
	static void run_job(int thread_id, MapReduceJob* job);
	static void map_phase(MapReduceJob* job, int  thread_id);
	static void shuffle_phase(MapReduceJob* job);
	static void reduce_phase(MapReduceJob* job);

	/**
	 * This fuction searches for the index in the intemidiate vectors that maximizes the key of the last elemnt in that vector.
	 * @param job the job with the intermidiate vecs that we are searching
	 * @param last_non_empty_vec_index the first index that we need to start the search at.
	 * @return the index of the intermidiate vector that has the largest key in its last element.
	 */
	static int MapReduceJob::find_max_key_pair(MapReduceJob *job, int last_non_empty_vec_index);
};


#endif // MAP_REDUCE_JOB_H
