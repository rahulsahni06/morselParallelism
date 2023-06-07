cmake_minimum_required(VERSION 3.24)
project(morsel_demo)

set(CMAKE_CXX_STANDARD 17)

add_executable(morsel_demo main.cpp Dispatcher.h Worker.h Work.h Results.h JobState.h)
