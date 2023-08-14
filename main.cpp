#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <iomanip>
#include "Dispatcher.h"
#include "Worker.h"
#include <chrono>
#include "timeUtils.h"

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

class Result {

public:
    int data1i;
    int data2i;
    std::string data1Name;
    std::string data2Name;

    Result(int data1I, int data2I, const std::string &data1Name, const std::string &data2Name) : data1i(data1I),
                                                                                                 data2i(data2I),
                                                                                                 data1Name(data1Name),
                                                                                                 data2Name(data2Name) {}

    void print() {
        std::cout<<data1i<<" "<<data2i<<" "<<data1Name<<" "<<data2Name<<std::endl;
    }

};


int main() {

    std::vector<Data> dataset;
    std::vector<Data2> probDataset;
    for(int i = 0; i<1000000; i++) {
//        probDataset.emplace_back(i, "data2_name"+ std::to_string(i));
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    for(int i = 0 ; i<100000; i+=5) {
//        dataset.emplace_back(i, "name" + std::to_string(i));
        probDataset.emplace_back(i, "data2_name"+ std::to_string(i));
    }

    std::cout<<"Starting dispatcher"<<std::endl;

    int morselSize = 1000;
    int noOfWorkers = 8;

    uint64_t startTime = timeSinceEpochMilliSec();

    Dispatcher<Data, Data2, int, Result> dispatcher(morselSize, noOfWorkers, dataset, probDataset);
    std::thread threads[noOfWorkers];
    std::vector<Worker<Data, Data2, int, Result>*> workers;


    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        std::cout<<i<<std::endl;
        Worker<Data, Data2, int, Result>* worker = new Worker<Data, Data2, int, Result>(i);
        workers.push_back(worker);
        threads[i] = worker->run(&dispatcher);
    }


    for(std::thread& t : threads) {
        t.join();
    }

    for(auto &w : workers) {
        delete w;
    }
//
    uint64_t endTime = timeSinceEpochMilliSec();
    std::cout<<"Total time taken: "<<std::setprecision(15)<<(endTime - startTime)/1000.0<<std::endl;

//    std::unordered_map<int, std::vector<Data>> map = dispatcher.getGlobalHasMap();
//    for(Data data1 : dataset) {
//        auto itr = map.find(data1.i);
//        if (itr == map.end()) {
//            std::cout<<data1.i<<" not found"<<std::endl;
//        }
//    }

//    std::vector<Results<Data, Data2>> results = dispatcher.getResults();
//    for(const Results<Data, Data2>& result : results) {
//        std::cout<<result.tSource.i<<"|"<<result.tSource.name<<"|"<<result.tProbeSource.name<<std::endl;
//    }

    auto results = dispatcher.getResults();
    std::cout<<"Results size:" << results.size();

}
