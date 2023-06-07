//
// Created by Rahul Sahni on 07/06/23.
//

#ifndef MORSEL_DEMO_WORKER_H
#define MORSEL_DEMO_WORKER_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include "Results.h"
#include "Dispatcher.h"
#include "JobState.h"
#include "Work.h"

template <class TSource, class TProbSource, class TResult, class THashKey>
class Worker {
public:

    int id;
    bool isAlive = true;

    Worker(int id) :id(id) {}

    void buildHashMap(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey>* dispatcher) {
        dispatcher->updateWorkerJobStatus(id, JobState::build);
        std::unordered_map<int, std::vector<TSource>> localMap;
        for(TSource tSource : work->buildMorsel) {
            localMap[tSource.i].push_back(tSource);
        }
        dispatcher->batchTransferToGlobalMap(localMap);
        dispatcher->updateWorkerJobStatus(id, JobState::buildDone);
    }

    void probeHashMap(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey>* dispatcher) {
        dispatcher->updateWorkerJobStatus(id, JobState::probe);
        std::vector<TResult> localResults;
        for(TProbSource probSource : work->probeMorsel) {
            std::vector<TSource> hashedResult = dispatcher->getHashedItem(probSource.i);
            if(hashedResult.size() > 0) {
                for(TSource tSource : hashedResult) {
                    Results<TSource, TProbSource> result(tSource, probSource);
                    localResults.push_back(result);
                }
            }
        }
        dispatcher->batchStoreResult(localResults);
        dispatcher->updateWorkerJobStatus(id, JobState::probeDone);
    }

    void start(Dispatcher<TSource, TProbSource, THashKey>* dispatcher1) {
        while(isAlive) {
            Work<TSource, TProbSource> work = dispatcher1->getWork();
            switch(work.jobState) {
                case JobState::build:
                    buildHashMap(&work, dispatcher1);
                    break;
                case JobState::waitingBuild:
                    //Wait for other threads to finish build phase
                    break;
                case JobState::probe:
//                    isAlive = false;
                    probeHashMap(&work, dispatcher1);
                    break;
                case JobState::waitingProbe:
                    //Wait for other threads to finish probe phase
                    break;
                case JobState::done:
                    isAlive = false;
                    break;
            }
        }
    }

    std::thread run(Dispatcher<TSource, TProbSource, THashKey> *dispatcher) {
        return std::thread(&Worker::start, this, dispatcher);
    }

};

#endif //MORSEL_DEMO_WORKER_H
