cmake_minimum_required(VERSION 3.10)

project(dikeLambda VERSION 0.1.0)

add_library(libdikeLambda STATIC 
                                 LambdaProcessorFactory.cpp
                                 LambdaProcessor.cpp
                                 LambdaProcessorReadAhead.cpp
                                 LambdaProcessorTotal.cpp 
                                 LambdaNode.cpp 
                                 LambdaFrame.cpp
                                 LambdaFilterNode.cpp
                                 LambdaAggregateNode.cpp)

include_directories(
        . 
        ../src
        ${CMAKE_SOURCE_DIR}/external/poco/Foundation/include 
        ${CMAKE_SOURCE_DIR}/external/poco/Util/include
        ${CMAKE_SOURCE_DIR}/external/poco/Net/include
        ${CMAKE_SOURCE_DIR}/external/poco/JSON/include
        )

find_package(OpenMP REQUIRED)

target_compile_options(libdikeLambda PRIVATE -fopenmp )
