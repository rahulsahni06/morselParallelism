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

public:

    std::mutex mutex;

    Dispatcher(int morselSize, int noOfWorkers, const std::vector<Data> &dataset) : morselSize(morselSize),
                                                                                 noOfWorkers(noOfWorkers),
                                                                                 dataset(dataset) {}

    int getMorselSize() {
        return morselSize;
    }

    int getNoOfWorkers() {
        return noOfWorkers;
    }

    Work<Data> getWork() {
        JobState jobState;
        jobState = build;
        Work<Data> work(build);
        morselStartIndex = morselEndIndex;
        morselEndIndex = morselStartIndex + morselSize;
        if(morselEndIndex >= dataset.size()) {
            morselEndIndex = dataset.size();
        }
        work.morsel = std::vector<Data>(dataset.begin() + morselStartIndex, dataset.begin() + morselEndIndex);

        return work;
    }

    void batchTransferToGlobalMap(std::unordered_map<int, std::vector<Data>> &localHashMap) {
        mutex.lock();
        for(std::pair pair : localHashMap) {
            globalHasMap[pair.first] = pair.second;
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


    void buildHashMap(Work<Data> work) {
        std::unordered_map<int, std::vector<Data>> localMap;
        for(Data data : work.morsel) {
            data.print();
            try {

                localMap[data.i].push_back(data);
            } catch (const std::exception &exc) {
                // catch anything thrown within try block that derives from std::exception
                std::cerr << exc.what();
            }
        }
        dispatcher->batchTransferToGlobalMap(localMap);
    }

    void start() {
        if(isAlive) {
            Work<Data> work = dispatcher->getWork();
            switch(work.jobState) {
                case build:
                    thread = std::thread(&Worker::buildHashMap, this, work);
                    break;
                case probe:
                    break;
                case done:
                    break;
            }
        }
    }
};



int main() {
    Data data(1, "123");
    Data data2(2, "1234");

    std::vector<Data> dataset;
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }
    for(int i = 0; i<1000; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    int morselSize = 10;
    int noOfWorkers = 8;

    Dispatcher dispatcher(morselSize, noOfWorkers, dataset);
    Work<Data> work = dispatcher.getWork();
    Worker worker(&dispatcher);
    worker.start();
//    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
//        Worker worker(&dispatcher);
//        worker.start();
//    }

    std::unordered_map<int, std::vector<Data>> map;
    for(Data data1: dataset) {
        map[data1.i].push_back(data1);
    }
}

