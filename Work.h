//
// Created by Rahul Sahni on 07/06/23.
//

#ifndef MORSEL_DEMO_WORK_H
#define MORSEL_DEMO_WORK_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fstream>
#include "JobState.h"

template <class TSource, class TProbSource>
class Work {
public:
    JobState jobState;
    std::vector<TSource> buildMorsel;
    std::vector<TProbSource> probeMorsel;

    Work(JobState state) {
        jobState = state;
    }

};

#endif //MORSEL_DEMO_WORK_H
