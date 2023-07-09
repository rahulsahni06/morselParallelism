cmake_minimum_required(VERSION 3.24)
project(morsel_demo)

set(CMAKE_CXX_STANDARD 17)

find_package(TBB REQUIRED)

add_executable(morsel_demo main.cpp Dispatcher.h Worker.h Work.h Results.h JobState.h timeUtils.h)
#target_include_directories(morsel_demo PUBLIC /home/rsahni/Documents/projects/oneTBB/build/include)
#target_link_libraries(morsel_demo PUBLIC /home/rsahni/Documents/projects/oneTBB/build/lib/libtbb.so)
target_link_libraries(morsel_demo TBB::tbb)