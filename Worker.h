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
#include "timeUtils.h"

template <class TSource, class TProbSource, class TResult, class THashKey>
class Worker {

    char logMode = 'd';
public:

    int id;
    bool isAlive = true;

    Worker(int id) :id(id) {
        logMessage(logMode, ("starting worker: "+std::to_string(id)));
    }

    void buildHashMap(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey>* dispatcher) {
        logMessage(logMode, ("build: "+std::to_string(id)));
        std::unordered_map<int, std::vector<TSource>> localMap;
        for(TSource tSource : work->buildMorsel) {
            localMap[tSource.i].push_back(tSource);
        }
        logMessage(logMode, ("build complete: "+std::to_string(id)));
        dispatcher->updateWorkerJobStatus(id, JobState::build);
        dispatcher->batchTransferToGlobalMap(localMap);
        dispatcher->updateWorkerJobStatus(id, JobState::buildDone);
    }

    void probeHashMap(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey>* dispatcher) {
        logMessage(logMode, ("probe: "+std::to_string(id)));
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
        dispatcher->updateWorkerJobStatus(id, JobState::probe);
        dispatcher->batchStoreResult(localResults);
        dispatcher->updateWorkerJobStatus(id, JobState::probeDone);
    }

    void start(Dispatcher<TSource, TProbSource, THashKey>* dispatcher1) {
        while(isAlive) {
//            logMessage(logMode, ("getting work: "+std::to_string(id)));
            Work<TSource, TProbSource> work = dispatcher1->getWork();
            switch(work.jobState) {
                case JobState::build:
                    logMessage(logMode, ("getting work: "+std::to_string(id)));
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
