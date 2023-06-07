#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include "Dispatcher.h"
#include "Worker.h"
#include "Results.h"

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
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    for(int i = 0 ; i<1000; i+=5) {
        probDataset.emplace_back(i, "data2 "+ std::to_string(i));
    }

    std::cout<<"Starting dispatcher"<<std::endl;

    int morselSize = 1000;
    int noOfWorkers = 8;

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
