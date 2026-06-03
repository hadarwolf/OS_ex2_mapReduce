#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H


#include "MapReduceClient.h"
#include <thread>
#include <barrier>
#include<algorithm>
#include <list>
#include <mutex>
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
	OutputVec output_vec_;
	const InputVec& input_vec_;
	std::vector<IntermediateVec> intermediate_vectors;
	std::list<IntermediateVec> shuffeld_intermidiate_vecs; // The intermidiate vectors after the shuffle phase.
	// will hold in first 2 bits the state of the job,
	// next 31 bit are the number of pairs to be proccessed at the current stage,
	// next 31 bits are the number of paires that were processed.
	std::atomic<uint64_t> atomic_input_counter;
	bool wait_wasnt_called = true;

	std::mutex pop_queue_mutex;
	std::mutex wait_mutex;
	std::barrier<> barrier_;


	ReduceContext reduce_context_; // deals with the
	std::vector<MapContext> map_context_; // holds and manages the intermidite pairs of each thread, each thread gets 1 map context.


	int num_intermidiate_pairs = 0;

	static const uint64_t get_num_pairs = 0x7fffffff;  // 7 in hex is 0011 in binary,
	static const uint64_t UNDEFINED_STATE = 0x0000000000000000;
	static const uint64_t MAP_STATE = 0x4000000000000000;    // 0100(bin) = 4(hex)
	static const uint64_t SHUFFLE_STATE = 0x8000000000000000; // 1000(bin) = 8(hex)
	static const uint64_t REDUCE_STATE = 0xC000000000000000;  // (1100)(bin) = C(hex)


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
