#include <string>
#include <sstream>
#include <chrono>
#include <stdio.h>
//#include <pthread.h>
#include <algorithm>
#include <vector>
#include <thread>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include "DikeUtil.hpp"
#include "LambdaProcessor.hpp"
#include "LambdaFrame.hpp"
#include "LambdaNode.hpp"
#include "LambdaFilterNode.hpp"

using namespace lambda;

void LambdaProcessorReadAhead::Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    std::string resp("LambdaProcessorReadAhead::Init " + dikeProcessorConfig["ID"] );
    std::cout << resp << std::endl;

    output->write(resp.c_str(), resp.length());
    return;

    LambdaProcessor * lambdaProcessor = new LambdaProcessor;
    lambdaProcessor->Init(dikeProcessorConfig, output);
    rowGroupCount = lambdaProcessor->rowGroupCount;

    lambdaResultVector = new LambdaResultVector(rowGroupCount, 2);

    std::thread workerThread = std::thread([=] { Worker(lambdaProcessor); });
}

int LambdaProcessorReadAhead::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{    
    int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);
    std::string resp("LambdaProcessorReadAhead::Run " + dikeProcessorConfig["ID"] );
    std::cout << resp << " " << rowGroupIndex << std::endl;
    return 0;
    
    LambdaResult * res = lambdaResultVector->resultVector[rowGroupIndex];
    lambdaResultVector->lock.lock();
    if (res->state == LambdaResult::READY) {
        res->state = LambdaResult::DONE;
        lambdaResultVector->lock.unlock();
        // Send results to output        
        for(int i = 0; i < res->buffers.size(); i++){
            output->write((const char * )res->buffers[i]->ptr, res->buffers[i]->len);
        }
    } else {
        lambdaResultVector->lock.unlock();
    }

    return 0;
}

void LambdaProcessorReadAhead::Worker(LambdaProcessor * lambdaProcessor)
{    
    int result_index = 0;
    // wait on lambdaResultVector->sem
    // Lock lambdaResultVector
    // Find next index to operate
    // Crank lambdaProcessor
    // Repeat 
}

