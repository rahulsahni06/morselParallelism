#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>

class Data {

public:
    int i;
    std::string name;

    Data(int i, std::string name) {
        this->i = i;
        this->name = name;
    }

    void print() {
        std::cout<<i<<" "<<name<<std::endl;
    }

};

enum JobState {
    build,
    probe,
    done
};


template <class T>
class Work {
public:
    JobState jobState;
    std::vector<Data> morsel;

    Work(JobState state) {
        jobState = state;
    }

};




class Dispatcher {

    int morselSize;
    int noOfWorkers;
    std::vector<Data> dataset;
    std::unordered_map<int, std::vector<Data>> globalHasMap;

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
    State dispatcherState = building;

    Dispatcher(int morselSize, int noOfWorkers, const std::vector<Data> &dataset) : morselSize(morselSize),
                                                                                    noOfWorkers(noOfWorkers),
                                                                                    dataset(dataset) {}

    int getMorselSize() {

        return morselSize;
    }

    int getNoOfWorkers() {
        return noOfWorkers;
    }

    const std::unordered_map<int, std::vector<Data>> getGlobalHasMap() {
        return globalHasMap;
    }

    Work<Data> getWork() {
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
            return work;
        }
        JobState jobState(probe);
        Work<Data> work(jobState);
        return work;
    }

    void batchTransferToGlobalMap(std::unordered_map<int, std::vector<Data>> &localHashMap) {
        mutex.lock();
        for(std::pair pair : localHashMap) {
            for(Data data : pair.second)
                globalHasMap[pair.first].push_back(data);
        }
        mutex.unlock();
    }

    void printMap() {
        for(auto pair : globalHasMap) {
            std::cout<<std::endl<<"Key: "<<pair.first<<" Size: "<<pair.second.size()<<std::endl;
            for(Data data : pair.second) {
                data.print();
            }
        }
    }

};

class Worker {
public:
    bool isAlive = true;
    std::thread thread;
    Dispatcher* dispatcher;

    Worker(Dispatcher *dispatcher) : dispatcher(dispatcher) {}


    void buildHashMap(Work<Data> *work) {
        std::unordered_map<int, std::vector<Data>> localMap;
        for(Data data : work->morsel) {
            localMap[data.i].push_back(data);
        }
        dispatcher->batchTransferToGlobalMap(localMap);
    }

    void start() {
        while(isAlive) {
            Work<Data> work = dispatcher->getWork();
            switch(work.jobState) {
                case build:
                    thread = std::thread(&Worker::buildHashMap, this, &work);
                    thread.join();
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
};



int main() {

    std::vector<Data> dataset;
    for(int i = 0; i<100; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }
    for(int i = 0; i<100; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }
    for(int i = 0; i<100; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    int morselSize = 10;
    int noOfWorkers = 4;

    Dispatcher dispatcher(morselSize, noOfWorkers, dataset);

    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        Worker worker(&dispatcher);
        worker.start();
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
