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
#include "Results.h"
#include "JobState.h"
#include "Work.h"

template <class TSource, class TProbSource, class THashKey>
class Dispatcher {

    int morselSize;
    int noOfWorkers;
    std::vector<TSource> dataset;
    std::vector<TProbSource> probeDataset;
    std::unordered_map<int, std::vector<TSource>> globalHasMap;
    std::vector<Results<TSource, TProbSource>> results;

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
                                                               probeDataset(probeDataset){}

    int getMorselSize() {
        return morselSize;
    }

    int getNoOfWorkers() {
        return noOfWorkers;
    }

    const std::unordered_map<int, std::vector<TSource>>& getGlobalHasMap() {
        return globalHasMap;
    }

    const std::vector<Results<TSource, TProbSource>>& getResults() {
        return results;
    }

    Work<TSource, TProbSource> getWork() {
        workMutex.lock();
        if(dispatcherState == Dispatcher::State::buildingMorsels) {
            JobState jobState(JobState::build);
            Work<TSource, TProbSource> work(jobState);
            morselStartIndex = morselEndIndex;
            morselEndIndex = morselStartIndex + morselSize;
            if(morselEndIndex >= dataset.size()) {
                morselEndIndex = dataset.size();
                dispatcherState = Dispatcher::State::probingMorsels;
            }
            work.buildMorsel = std::vector<TSource>(dataset.begin() + morselStartIndex, dataset.begin() + morselEndIndex);
            if(dispatcherState == Dispatcher::State::probingMorsels) {
                morselStartIndex = 0;
                morselEndIndex = 0;
            }
            workMutex.unlock();
            return work;
        }
        if(dispatcherState == Dispatcher::State::probingMorsels) {
            if(isBuildDone()) {
                JobState jobState(JobState::probe);
                Work<TSource, TProbSource> work(jobState);
                morselStartIndex = morselEndIndex;
                morselEndIndex = morselStartIndex + morselSize;
                if(morselEndIndex >= probeDataset.size()) {
                    morselEndIndex = probeDataset.size();
                    dispatcherState = Dispatcher::State::doneProbingMorsels;
                }
                work.probeMorsel = std::vector<TProbSource>(probeDataset.begin() + morselStartIndex, probeDataset.begin() + morselEndIndex);
                workMutex.unlock();
                return work;
            } else {
                JobState jobState(JobState::waitingBuild);
                Work<TSource, TProbSource> work(jobState);
                workMutex.unlock();
                return work;
            }

        }
        if(dispatcherState == Dispatcher::State::doneProbingMorsels) {
            if(isProbeDone()) {
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

    std::vector<TSource> getHashedItem(THashKey tHashKey) {
        readHashedItemMutex.lock();

        auto itr = globalHasMap.find(tHashKey);
        if (itr == globalHasMap.end()) {
            readHashedItemMutex.unlock();
            std::vector<TSource> emptyResult;
            return emptyResult;
        } else {
            readHashedItemMutex.unlock();
            return itr->second;
        }
    }

    void batchTransferToGlobalMap(std::unordered_map<int, std::vector<TSource>> &localHashMap) {
        readHashedItemMutex.lock();
        for(std::pair pair : localHashMap) {
            for(TSource tSource : pair.second)
                globalHasMap[pair.first].push_back(tSource);
        }
        readHashedItemMutex.unlock();
    }

    void batchStoreResult(std::vector<Results<TSource, TProbSource>> localResults) {
        resultMutex.lock();
        results.insert(results.end(),localResults.begin(),localResults.end());
        resultMutex.unlock();
    }

    void printMap() {
        for(auto pair : globalHasMap) {
            std::cout<<std::endl<<"Key: "<<pair.first<<" Size: "<<pair.second.size()<<std::endl;
            for(TSource data : pair.second) {
                data.print();
            }
        }
    }

    void updateWorkerJobStatus(int workerId, JobState jobState) {
        workerStatusMutex.lock();
        workersJobState[workerId] = jobState;
        workerStatusMutex.unlock();
    }

    bool isBuildDone() {
        bool isDone = true;
        for(std::pair pair : workersJobState) {
            isDone = isDone && (pair.second == JobState::buildDone || pair.second == JobState::probe
                                || pair.second == JobState::probeDone || pair.second == JobState::done);
        }
        return isDone;
    }

    bool isProbeDone() {
        bool isDone = true;
        for(std::pair pair : workersJobState) {
            isDone = isDone && (pair.second == probeDone);
        }
        return isDone;
    }
};
#endif //MORSEL_DEMO_DISPATCHER_H
