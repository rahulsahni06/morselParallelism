#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include <iomanip>
#include "Dispatcher.h"
#include "Worker.h"
#include "Results.h"
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


int main() {

    std::vector<Data> dataset;
    std::vector<Data2> probDataset;
    for(int i = 0; i<1000000; i++) {
        probDataset.emplace_back(i, "data2_name"+ std::to_string(i));
//        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    for(int i = 0 ; i<100000; i+=5) {
        dataset.emplace_back(i, "name" + std::to_string(i));
//        probDataset.emplace_back(i, "data2_name"+ std::to_string(i));
    }

    std::cout<<"Starting dispatcher"<<std::endl;

    int morselSize = 1000;
    int noOfWorkers = 4;

    uint64_t startTime = timeSinceEpochMilliSec();

    Dispatcher<Data, Data2, int> dispatcher(morselSize, noOfWorkers, dataset, probDataset);
    std::thread threads[noOfWorkers];
    std::vector<Worker<Data, Data2, Results<Data, Data2>, int>*> workers;


    for(int i = 0; i<dispatcher.getNoOfWorkers(); i++) {
        std::cout<<i<<std::endl;
        Worker<Data, Data2, Results<Data, Data2>, int>* worker = new Worker<Data, Data2, Results<Data, Data2>, int>(i);
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
    std::cout<<"time: "<<std::setprecision(15)<<(endTime - startTime)/1000.0;

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

}
