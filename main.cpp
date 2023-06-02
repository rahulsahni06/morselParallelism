#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>

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
    int noOfWorkers = 8;
    int morselSize = 10;
    std::vector<Data> dataset;
    std::unordered_map<int, std::vector<Data>> globalHasMap;

    int morselStartIndex = 0;
    int morselEndIndex = 0;

public:

    Dispatcher() {

    }

    Dispatcher(std::vector<Data> dataset) {
        this->dataset = dataset;
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
        work.morsel = std::vector<Data>(dataset.begin() + morselStartIndex, dataset.begin() + morselEndIndex - 1);

        return work;
    }

};

class Worker {
public:
    bool isAlive = true;
    std::thread thread;
    std::unordered_map<int, std::vector<Data>> localMap;
    Dispatcher dispatcher;

    Worker(Dispatcher dispatcher) {
        this->dispatcher = dispatcher;
    }

    static void buildHashMap(Work<Data> work, std::unordered_map<int, std::vector<Data>> localMap) {
        for(Data data : work.morsel) {
            localMap[data.i].push_back(data);
        }
    }

    void start() {
        if(isAlive) {
            Work<Data> work = dispatcher.getWork();
            switch(work.jobState) {
                case build:
                    thread = std::thread(&Worker::buildHashMap, this, localMap);
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
    std::vector<Data> dataset;
    for(int i = 0; i<10; i++) {
        dataset.emplace_back(i, "name" + std::to_string(i));
    }

    for(Data data : dataset) {
        data.print();
    }
}

