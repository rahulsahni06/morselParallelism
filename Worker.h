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
#include "Dispatcher.h"
#include "JobState.h"
#include "Work.h"
#include "timeUtils.h"

template<class TSource, class TProbSource, class THashKey, class TResult>
class Worker {

    char logMode = 'd';
public:

    int id;
    bool isAlive = true;

    Worker(int id) :id(id) {
        logMessage(logMode, ("starting worker: "+std::to_string(id)));
    }

    void buildHashMap2(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey, TResult>* dispatcher) {
//        logMessage(logMode, ("build: "+std::to_string(id)));
        dispatcher->updateWorkerJobStatus(id, JobState::build);
        auto t = dispatcher->getDataset();
        for(int i = work->startIdx; i<work->endIdx; i++) {
            auto d = t->at(i);
            dispatcher->addToHashMap(d, d.i);

        }
        dispatcher->updateWorkerJobStatus(id, JobState::buildDone);
//        logMessage(logMode, ("build complete: "+std::to_string(id)));
    }

    void probeHashMap2(Work<TSource, TProbSource> *work, Dispatcher<TSource, TProbSource, THashKey, TResult>* dispatcher) {
//        logMessage(logMode, ("probe: "+std::to_string(id)));
        dispatcher->updateWorkerJobStatus(id, JobState::probe);
        auto p = dispatcher->getProbeDataset();
        for(int i = work->startIdx; i<work->endIdx; i++) {
            auto probSource = p->at(i);
            std::vector<TSource>* hashedResult = dispatcher->getHashedItem3(probSource.i);
            if(hashedResult != nullptr && hashedResult->size() > 0) {
                for(TSource& tSource : *hashedResult) {
                    TResult t(tSource.i, probSource.i, tSource.name, probSource.name);
                    dispatcher->storeResult2(t);
                }
            }
        }
        dispatcher->updateWorkerJobStatus(id, JobState::probeDone);
//        logMessage(logMode, ("probe complete: "+std::to_string(id)));
    }


    void start(Dispatcher<TSource, TProbSource, THashKey, TResult> *dispatcher1) {
        while (isAlive) {
            Work<TSource, TProbSource> work = dispatcher1->getWork();
            switch (work.jobState) {
                case JobState::build:
                    buildHashMap2(&work, dispatcher1);
                    break;
                case JobState::waitingBuild:
                    //Wait for other threads to finish build phase
                    break;
                case JobState::probe:
//                    isAlive = false;
                    probeHashMap2(&work, dispatcher1);
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

    std::thread run(Dispatcher<TSource, TProbSource, THashKey, TResult> *dispatcher) {
        return std::thread(&Worker::start, this, dispatcher);
    }

};

#endif //MORSEL_DEMO_WORKER_H
