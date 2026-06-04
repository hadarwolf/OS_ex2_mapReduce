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

	/**
	 * Constructor to the object that completes the mapReduce job.
	 * The Job satrts to be executed when creating this object.
	 *
	 * @param client the client that runs the map and reduce functions
	 * @param inputVec the input data to be processed.
	 * @param multiThreadLevel the number of threads wanted to process the data
	 */
	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	/**
	 * Destructor: it waits till the job is done before destroying the object
	 */
	~MapReduceJob();

	/**
	 * Returns the state of the job.
	 *
	 * @return The state the job is in (what stage, and how many percent of that stage was completed)
	 */
	MapReduceState getState(void) const;

	/**
	 * A bool checker that checks if the job is done.
	 * @return a boolean value indicating if the job is finished or not
	 */
	bool isDone(void) const;

	/**
	 * The caller of this function gets stuck in this method till the job is done.
	 */
	void wait(void);

	/**
	 * Returns the result of the job, if when this function is called the job was not done
	 * then the caller of this function will get stuck in this function till the job is done
	 * and he can get a valid return
	 *                                                         
	 * @return The output vector from the MapReduce algorithm
	 */
	OutputVec getOutput(void);

private:
	friend class std::thread; // allow threads to have access to private fields and methods.


	//************************************//
	//************** FIELDS **************//
	//************************************//

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
	std::mutex pop_queue_mutex;
	std::mutex wait_mutex;
	std::barrier<> barrier_;

	bool wait_wasnt_called = true;

	ReduceContext reduce_context_; // deals with filling the output_vec

	uint64_t num_intermidiate_pairs = 0;

	static const uint64_t get_num_pairs = 0x7fffffff;  // 7 in hex is 0011 in binary,
	static const uint64_t UNDEFINED_STATE = 0x0000000000000000;
	static const uint64_t MAP_STATE = 0x4000000000000000;    // 0100(bin) = 4(hex)
	static const uint64_t SHUFFLE_STATE = 0x8000000000000000; // 1000(bin) = 8(hex)
	static const uint64_t REDUCE_STATE = 0xC000000000000000;  // (1100)(bin) = C(hex)


	//************************************//
	//************** FUNCTIONS **************//
	//************************************//

	/**
	 * The launce function of each thread, that does the job acording to the MapReduce algorithm.
	 *
	 * @param thread_id the thread id that starts in this function
	 * @param job the current job.
	 */
	static void run_job(int thread_id, MapReduceJob* job);

	/**
	 * The map phase includes poping pairs out of the input vec and activating the map method on them.
	 *
	 * @param thread_id the thread id that starts in this function
	 * @param job the current job.
	 */
	static void map_phase(MapReduceJob* job, int  thread_id);

	/**
	 * The shuffle phase is done only by thread 0, and include reorgenizing all the output from the map phase
	 * into vectors, such that each key gets a vector with all the values with that key.
	 *
	 * @param job the current job.
	 */
	static void shuffle_phase(MapReduceJob* job);

	/**
	 *The reduce phase is done by all threads and include taking a vector from the shuffle phase and aplying the
	 * reduce function on it.
	 *
	 * @param job the current job.
	 */
	static void reduce_phase(MapReduceJob* job);

	/**
	 * DOes the calculation of how many overall pairs are there in the
	 * intermidiate stage( after the map and before the end of the reduce)
	 */
	void calculate_num_intermidiate_pairs();

	/**
	 * This fuction searches for the index in the intemidiate vectors that maximizes the key of the last elemnt in that vector.
	 * @param job the job with the intermidiate vecs that we are searching
	 * @param last_non_empty_vec_index the first index that we need to start the search at.
	 * @return the index of the intermidiate vector that has the largest key in its last element.
	 */
	static int find_max_key_pair(MapReduceJob *job, int last_non_empty_vec_index);
};


#endif // MAP_REDUCE_JOB_H
