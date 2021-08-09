#ifndef DIKE_PROCESSOR_HPP
#define DIKE_PROCESSOR_HPP

#include <iostream>
#include <string.h>

#include "DikeAsyncWriter.hpp"

typedef std::map<std::string, std::string> DikeProcessorConfig;

class DikeProcessor {    
    public:
    DikeProcessor(){};

    virtual ~DikeProcessor() {
        if(workerThread.joinable()) {
            workerThread.join();
        }
    }

    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) = 0;
    
    DikeAsyncWriter * dikeWriter = NULL;
    std::thread workerThread;    
    uint64_t record_counter = 0;
    bool isRunning;

    std::thread startWorker() {
        return std::thread([this] { this->Worker(); });
    }

    virtual void Worker() = 0;
};

#endif /* DIKE_PROCESSOR_HPP */