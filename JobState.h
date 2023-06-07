//
// Created by Rahul Sahni on 07/06/23.
//

#ifndef MORSEL_DEMO_JOBSTATE_H
#define MORSEL_DEMO_JOBSTATE_H

enum JobState {
    build, //buildingMorsels hashtable
    waitingBuild, //waiting for other workers to finish buildingMorsels hashtable
    buildDone, //hashtable built by all workers
    probe, //probing hashtable
    waitingProbe,//waiting for other workers to finish buildingMorsels hashtable
    probeDone,//probing finished by all workers
    done
};

#endif //MORSEL_DEMO_JOBSTATE_H
