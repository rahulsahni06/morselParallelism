#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>

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

enum JobState {
    build,
    buildDone,
    probe,
    done
};


template <class TSource>
class Work {
public:
    JobState jobState;
    std::vector<TSource> morsel;

    Work(JobState state) {
        jobState = state;
    }

};



template <class TSource, class TProbSource>
class Dispatcher {

    int morselSize;
    int noOfWorkers;
    std::vector<TSource> dataset;
    std::vector<TProbSource> probeDataset;
    std::unordered_map<int, std::vector<TSource>> globalHasMap;

    int morselStartIndex = 0;
    int morselEndIndex = 0;

    enum State {
        building,
        doneBuilding,
        probing,
        doneProbing
    };



public:

    std::mutex mutex;
    std::mutex mutex2;
    std::mutex workerStatusMutex;
    State dispatcherState = building;
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

    Work<TSource> getWork() {
        mutex2.lock();
        if(dispatcherState == building) {
            JobState jobState(build);
            Work<Data> work(jobState);
            morselStartIndex = morselEndIndex;
            morselEndIndex = morselStartIndex + morselSize;
            if(morselEndIndex >= dataset.size()) {
                morselEndIndex = dataset.size();
                dispatcherState = doneBuilding;
            }
            work.morsel = std::vector<Data>(dataset.begin() + morselStartIndex, dataset.begin() + morselEndIndex);
            mutex2.unlock();
            return work;
        }
        JobState jobState(probe);
        Work<Data> work(jobState);
        mutex2.unlock();
        return work;
    }

    void batchTransferToGlobalMap(std::unordered_map<int, std::vector<TSource>> &localHashMap) {
        mutex.lock();
        for(std::pair pair : localHashMap) {
            for(TSource tSource : pair.second)
                globalHasMap[pair.first].push_back(tSource);
        }
        mutex.unlock();
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

};

template <class TSource, class TProbSource>
class Worker {
public:

    int id;
    bool isAlive = true;
//    std::thread thread;

    Worker(int id) :id(id) {}


    void buildHashMap(Work<TSource> *work, Dispatcher<TSource, TProbSource>* dispatcher1) {
        dispatcher1->updateWorkerJobStatus(id, JobState::build);
        std::cout<<std::endl<<"runnning thread id "<<id<<std::endl;
        std::unordered_map<int, std::vector<TSource>> localMap;
        for(TSource tSource : work->morsel) {
            localMap[tSource.i].push_back(tSource);
        }
        dispatcher1->batchTransferToGlobalMap(localMap);
        dispatcher1->updateWorkerJobStatus(id, JobState::buildDone);
    }

    void start(Dispatcher<TSource, TProbSource>* dispatcher1) {
        while(isAlive) {
            Work<TSource> work = dispatcher1->getWork();
            switch(work.jobState) {
                case build:
                    buildHashMap(&work, dispatcher1);
                    break;
                case probe:
                    isAlive = false;
                    break;
                case done:
                    isAlive = false;
                    break;
            }
        }
    }


    std::thread run(Dispatcher<TSource, TProbSource> *dispatcher) {
        return std::thread(&Worker::start, this, dispatcher);

    }

    void join() {
//        thread.join();
    }

};



int main() {

    std::vector<Data> dataset;
    std::vector<Data2> probDataset;
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
        probDataset.emplace_back(i, "data2 "+ std::to_string(i));
    }
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
        probDataset.emplace_back(i, "data2 "+ std::to_string(i));
    }
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
        probDataset.emplace_back(i, "data2 "+ std::to_string(i));
    }

    int morselSize = 1000;
    int noOfWorkers = 4;

    Dispatcher dispatcher(morselSize, noOfWorkers, dataset, probDataset);
    std::thread threads[noOfWorkers];

    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        Worker<Data, Data2> worker( i);
        threads[i] = worker.run(&dispatcher);
    }

    for(std::thread& t : threads) {
        t.join();
    }

    std::unordered_map<int, std::vector<Data>> map = dispatcher.getGlobalHasMap();
    for(Data data1 : dataset) {
        auto itr = map.find(data1.i);
        if (itr == map.end()) {
            std::cout<<data1.i<<" not found"<<std::endl;
        } else {

        }
    }

    std::cout<<"Done";
}
