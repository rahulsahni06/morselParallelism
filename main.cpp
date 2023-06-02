#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>

class Data {
    int i;
    std::string name;

public:
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
    wait,
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
        morselStartIndex = 0;
        morselEndIndex = morselStartIndex + morselSize;
        if(morselEndIndex >= dataset.size()) {
            morselEndIndex = dataset.size();
        }
    }

    Work<Data> getWork() {
        JobState jobState;
        jobState = build;
        Work<Data> work(build);
        work.morsel = std::vector<Data>(dataset.begin() + morselStartIndex, dataset.begin() + morselEndIndex - 1);
        morselStartIndex = morselEndIndex;
        morselEndIndex = morselStartIndex + morselSize;
        if(morselEndIndex >= dataset.size()) {
            morselEndIndex = dataset.size();

        }
        return work;
    }

};

class Worker {
public:
    bool isAlive = true;
    std::thread thread;
    std::unordered_map<int, std::vector<Data>> localMap;
    Dispatcher dispatcher;

    Worker(Dispatcher dispatcher1) {
        this->dispatcher = dispatcher1;
    }

    void start() {
        if(isAlive) {
            //get work from dispatcher
            //start the work with thread
            thread = std::thread();
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

