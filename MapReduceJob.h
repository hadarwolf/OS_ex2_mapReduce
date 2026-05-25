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
	MapReduceState  job_state_;
	std::vector<std::thread> threads;
};
	
#endif // MAP_REDUCE_JOB_H
