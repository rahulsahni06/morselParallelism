//
// Created by rsahni on 07/07/23.
//

#ifndef MORSEL_DEMO_TIMEUTILS_H
#define MORSEL_DEMO_TIMEUTILS_H

#include <chrono>
#include <iostream>
#include <iomanip>

uint64_t timeSinceEpochMilliSec() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void logTimeTaken(uint64_t startTime, uint64_t endTime, std::string msg) {
    std::cout<<msg<<std::setprecision(15)<<(endTime - startTime)/1000.0<<std::endl;
}

//mode: 'd'
void logMessage(char mode, std::string msg) {
    if(mode == 'd') {
        std::cout<<msg<<std::endl;
    }
}
#endif //MORSEL_DEMO_TIMEUTILS_H
