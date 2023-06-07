//
// Created by Rahul Sahni on 07/06/23.
//

#ifndef MORSEL_DEMO_RESULTS_H
#define MORSEL_DEMO_RESULTS_H

template <class TSource, class TProbeSource>
class Results {
public:
    TSource tSource;
    TProbeSource tProbeSource;

public:
    Results(TSource tSource, TProbeSource tProbeSource) : tSource(tSource),
                                                          tProbeSource(tProbeSource) {}

};

#endif //MORSEL_DEMO_RESULTS_H
