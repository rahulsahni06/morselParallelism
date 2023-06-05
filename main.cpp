#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>

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



template <class TSource>
class Dispatcher {

    int morselSize;
    int noOfWorkers;
    std::vector<TSource> dataset;
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
    State dispatcherState = building;

    Dispatcher(int morselSize, int noOfWorkers, const std::vector<TSource> &dataset) : morselSize(morselSize),
                                                                                    noOfWorkers(noOfWorkers),
                                                                                    dataset(dataset) {}

    int getMorselSize() {

        return morselSize;
    }

    int getNoOfWorkers() {
        return noOfWorkers;
    }

    const std::unordered_map<int, std::vector<TSource>> getGlobalHasMap() {
        return globalHasMap;
    }

    Work<Data> getWork() {
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

};

template <class TSource>
class Worker {
public:

    int id;
    bool isAlive = true;
    std::thread thread;
    Dispatcher<TSource>* dispatcher;

    Worker(Dispatcher<TSource> *dispatcher, int id) : dispatcher(dispatcher), id(id) {
    }


    void buildHashMap(Work<TSource> *work) {
        std::cout<<std::endl<<"runnning thread id "<<id<<std::endl;
        std::unordered_map<int, std::vector<TSource>> localMap;
        for(TSource tSource : work->morsel) {
            localMap[tSource.i].push_back(tSource);
        }
        dispatcher->batchTransferToGlobalMap(localMap);
    }

    void start() {
        while(isAlive) {
            Work<Data> work = dispatcher->getWork();
            switch(work.jobState) {
                case build:
                    buildHashMap(&work);
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


    void run() {
        thread = std::thread(&Worker::start, this);

    }

    void join() {
        thread.join();
    }

};



int main() {

    std::vector<Data> dataset;
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    int morselSize = 10;
    int noOfWorkers = 4;

    Dispatcher dispatcher(morselSize, noOfWorkers, dataset);
    std::vector<std::thread> threads;
    std::vector<Worker<Data>*> workers;

    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        Worker<Data> worker(&dispatcher, i);
        worker.run();
        workers.push_back(&worker);
    }

   for(Worker<Data>* w : workers) {
       w->join();
       delete w;
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
