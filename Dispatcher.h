//
// Created by Rahul Sahni on 07/06/23.
//

#ifndef MORSEL_DEMO_DISPATCHER_H
#define MORSEL_DEMO_DISPATCHER_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include "JobState.h"
#include "Work.h"
#include "timeUtils.h"

#include "oneapi/tbb/concurrent_hash_map.h"
#include "oneapi/tbb/blocked_range.h"
#include "oneapi/tbb/parallel_for.h"
#include "oneapi/tbb/tick_count.h"
#include "oneapi/tbb/tbb_allocator.h"
#include "oneapi/tbb/global_control.h"
#include "oneapi/tbb/concurrent_vector.h"


template <class TSource, class TProbSource, class THashKey, class TResult>
class Dispatcher {

    typedef oneapi::tbb::concurrent_hash_map<int, std::vector<TSource>> ConcurrentHashMap;
    typedef oneapi::tbb::concurrent_vector<TResult> ConcurrentVector;


    int morselSize;
    int noOfWorkers;
    std::vector<TSource> dataset;
    std::vector<TProbSource> probeDataset;
    std::unordered_map<THashKey, std::vector<TSource>> globalHasMap;
    ConcurrentHashMap globalHashMap;
    ConcurrentVector results;

    uint64_t buildStartTime;
    uint64_t probeStartTime;
    bool isBuildTimeLogged = false;
    bool isProbeTimeLogged = false;

    int morselStartIndex = 0;
    int morselEndIndex = 0;


    enum State {
        buildingMorsels,
        probingMorsels,
        doneProbingMorsels,
        done
    };



public:

    std::mutex hashMapMutex;
    std::mutex workMutex;
    std::mutex workerStatusMutex;
    std::mutex resultMutex;
    std::mutex readHashedItemMutex;

    State dispatcherState = buildingMorsels;
    std::unordered_map<int, JobState> workersJobState;

    Dispatcher(int morselSize, int noOfWorkers, const std::vector<TSource> &dataset,
               const std::vector<TProbSource> &probeDataset) : morselSize(morselSize),
                                                               noOfWorkers(noOfWorkers),
                                                               dataset(dataset),
                                                               probeDataset(probeDataset){
        buildStartTime = timeSinceEpochMilliSec();
    }

    int getMorselSize() {
        return morselSize;
    }

    int getNoOfWorkers() {
        return noOfWorkers;
    }

    const std::vector<TSource>* getDataset() const {
        return &dataset;
    }

    const std::vector<TProbSource>* getProbeDataset() const {
        return &probeDataset;
    }

    const std::unordered_map<int, std::vector<TSource>> &getGlobalHasMap() {
        return globalHasMap;
    }

    const ConcurrentVector &getResults() const {
        return results;
    }

    Work<TSource, TProbSource> getWork() {
        workMutex.lock();
        if (dispatcherState == Dispatcher::State::buildingMorsels) {
            JobState jobState(JobState::build);
            Work<TSource, TProbSource> work(jobState);
            morselStartIndex = morselEndIndex;
            morselEndIndex = morselStartIndex + morselSize;
            if (morselEndIndex >= dataset.size()) {
                morselEndIndex = dataset.size();
                dispatcherState = Dispatcher::State::probingMorsels;
            }
            work.buildMorsel = std::vector<TSource>(dataset.begin() + morselStartIndex,
                                                    dataset.begin() + morselEndIndex);
            work.startIdx = morselStartIndex;
            work.endIdx = morselEndIndex;
            if (dispatcherState == Dispatcher::State::probingMorsels) {
                morselStartIndex = 0;
                morselEndIndex = 0;
            }
            workMutex.unlock();
            return work;
        }
        if (dispatcherState == Dispatcher::State::probingMorsels) {
            if (isBuildDone()) {
//                std::cout<<"Build done"<<std::endl;
                if(!isBuildTimeLogged) {
                    uint64_t endTime = timeSinceEpochMilliSec();
                    std::cout<<"Build done"<<std::endl;
                    logTimeTaken(buildStartTime, endTime, "Build time: ");
                    probeStartTime = timeSinceEpochMilliSec();
                    isBuildTimeLogged = true;
                }
                JobState jobState(JobState::probe);
                Work<TSource, TProbSource> work(jobState);
                morselStartIndex = morselEndIndex;
                morselEndIndex = morselStartIndex + morselSize;
                if (morselEndIndex >= probeDataset.size()) {
                    morselEndIndex = probeDataset.size();
                    dispatcherState = Dispatcher::State::doneProbingMorsels;
                }
//                work.probeMorsel = std::vector<TProbSource>(probeDataset.begin() + morselStartIndex,
//                                                            probeDataset.begin() + morselEndIndex);
                work.startIdx = morselStartIndex;
                work.endIdx = morselEndIndex;
                workMutex.unlock();
                return work;
            } else {
                JobState jobState(JobState::waitingBuild);
                Work<TSource, TProbSource> work(jobState);
                workMutex.unlock();
                return work;
            }

        }
        if (dispatcherState == Dispatcher::State::doneProbingMorsels) {
            if (isProbeDone()) {
                uint64_t endTime = timeSinceEpochMilliSec();
                std::cout<<"Probe done"<<std::endl;
                logTimeTaken(probeStartTime, endTime, "Probe time: ");

                dispatcherState = Dispatcher::State::done;
                JobState jobState(JobState::done);
                Work<TSource, TProbSource> work(jobState);
                workMutex.unlock();
                return work;
            } else {
                JobState jobState(JobState::waitingProbe);
                Work<TSource, TProbSource> work(jobState);
                workMutex.unlock();
                return work;
            }
        }
        dispatcherState = Dispatcher::State::done;
        JobState jobState(JobState::done);
        Work<TSource, TProbSource> work(jobState);
        workMutex.unlock();
        return work;
    }

    void addToHashMap(TSource& data, THashKey& hashKey) {
        typename ConcurrentHashMap::accessor a;
        globalHashMap.insert(a, hashKey);
        a->second.push_back(data);
    }

    std::vector<TSource>* getHashedItem3(THashKey& tHashKey) {
        typename ConcurrentHashMap::accessor a;
        globalHashMap.find(a, tHashKey);
        if(a.empty()) {
            return nullptr;
        }
        return &(a->second);

    }

    void storeResult2(TResult& tuple) {
        results.push_back(tuple);
    }


    void updateWorkerJobStatus(int workerId, JobState jobState) {
        workerStatusMutex.lock();
        workersJobState[workerId] = jobState;
        workerStatusMutex.unlock();
    }

    bool isBuildDone() {
        bool isDone = true;
        for (std::pair pair: workersJobState) {
            isDone = isDone && (pair.second == JobState::buildDone || pair.second == JobState::probe
                                || pair.second == JobState::probeDone || pair.second == JobState::done);
        }
        return isDone;
    }

    bool isProbeDone() {
        bool isDone = true;
        for (std::pair pair: workersJobState) {
            isDone = isDone && (pair.second == JobState::probeDone || pair.second == JobState::waitingProbe);
        }
        return isDone;
    }
};
#endif //MORSEL_DEMO_DISPATCHER_H
