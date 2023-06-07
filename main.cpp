#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>

class Data {

public:
    int i;
    std::string name;

    Data(int i, const std::string &name) : i(i), name("Data_"+name) {}

    void print() {
        std::cout<<i<<" "<<name<<std::endl;
    }

};

class Data2 {

public:
    int i;
    std::string name;

    Data2(int i, const std::string &name) : i(i), name("Data2_"+name) {}

    void print() {
        std::cout<<i<<" "<<name<<std::endl;
    }

};

template <class TSource, class TProbeSource>
class Results {
public:
    TSource tSource;
    TProbeSource tProbeSource;

public:
    Results(TSource tSource, TProbeSource tProbeSource) : tSource(tSource),
                                                                        tProbeSource(tProbeSource) {}

};

enum JobState {
    build, //buildingMorsels hashtable
    waitingBuild, //waiting for other workers to finish buildingMorsels hashtable
    buildDone, //hashtable built by all workers
    probe, //probing hashtable
    waitingProbe,//waiting for other workers to finish buildingMorsels hashtable
    probeDone,//probing finished by all workers
    done
};


template <class TSource, class TProbSource>
class Work {
public:
    JobState jobState;
    std::vector<TSource> buildMorsel;
    std::vector<TProbSource> probeMorsel;

    Work(JobState state) {
        jobState = state;
    }

};



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

    const std::unordered_map<int, std::vector<TSource>> getGlobalHasMap() {
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



int main() {

    std::vector<Data> dataset;
    std::vector<Data2> probDataset;
    for(int i = 0; i<10000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    for(int i = 0 ; i<100; i+=5) {
        probDataset.emplace_back(i, "data2 "+ std::to_string(i));
    }

    int morselSize = 10;
    int noOfWorkers = 1;

    Dispatcher<Data, Data2, int> dispatcher(morselSize, noOfWorkers, dataset, probDataset);
    std::thread threads[noOfWorkers];

    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        Worker<Data, Data2, Results<Data, Data2>, int> worker( i);
        threads[i] = worker.run(&dispatcher);
    }

    for(std::thread& t : threads) {
        t.join();
    }

//    std::unordered_map<int, std::vector<Data>> map = dispatcher.getGlobalHasMap();
//    for(Data data1 : dataset) {
//        auto itr = map.find(data1.i);
//        if (itr == map.end()) {
//            std::cout<<data1.i<<" not found"<<std::endl;
//        }
//    }

    std::vector<Results<Data, Data2>> results = dispatcher.getResults();
    for(const Results<Data, Data2>& result : results) {
        std::cout<<result.tSource.i<<"|"<<result.tSource.name<<"|"<<result.tProbeSource.name<<std::endl;
    }

}
