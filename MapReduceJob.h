#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include <thread>

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

	
	MapReduceState  job_state_;
	std::unique_ptr<std::thread[]> threads;  // pointer to the thread array
	std::unique_ptr<IntermediateVec[]> intermediate_vectors;  //pointer to the intermidiate array
	OutputVec output_vec_;
	const InputVec& input_vec_;
	std::atomic<uint64_t>* atomic_input_counter; // will hold in first 2 bits the state ofnthe job,
												// next 31 bit are the number of pairs proccessed in input_vec,
												//next 31 bits are the number of paires left to proccess in input_vec.


	/**
	 *
	 * @param thread_id the thread id that starts in this function
	 * @param job the current job.
	 * @param start iterator to the start of the segmnet of the input this thread needs to handle.
	 * @param end   iterator to the end   of the segmnet of the input this thread needs to handle.
	 */
	static void run_job(int thread_id, MapReduceJob* job,
	                    InputVec* input_vec); // TODO maby have the jobs grab pairs from the start vector instead of deviding it equaly, maby will be more efiecient, on the other hand we will need to hold a mutex to prevent access to the resource simotaniasly.
};
	
#endif // MAP_REDUCE_JOB_H
