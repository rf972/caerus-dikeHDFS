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
    std::string resp("LambdaProcessorReadAhead::Init [" + dikeProcessorConfig["ID"] + "]");
    //std::cout << resp << std::endl;

    output->write(resp.c_str(), resp.length());

    int nWorkers = std::stoi(dikeProcessorConfig["dike.storage.processor.workers"]);
    workerThread.resize(nWorkers);
    
    for(int i = 0; i < workerThread.size(); i++) {
        LambdaProcessor * lambdaProcessor = new LambdaProcessor;    
        lambdaProcessor->Init(dikeProcessorConfig, NULL); // TODO get rid of the output
        
        if(i == 0){
            rowGroupCount = lambdaProcessor->rowGroupCount;
            totalResults = rowGroupCount;
            lambdaResultVector = new LambdaResultVector(rowGroupCount, 2*nWorkers);
        }
        workerThread[i] = std::thread([=] { Worker(lambdaProcessor); });
    }
}

// This will simply send back results
int LambdaProcessorReadAhead::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{    
    int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);

    int loopback =  std::stoi(dikeProcessorConfig["dike.storage.processor.loopback"]);

    if(loopback == 1) {
        LambdaResult * res = lambdaResultVector->resultVector[0];
        sem_wait(&res->sem); // This will guarantee that results are available
        // Send results to output        
        for(int i = 0; i < res->buffers.size(); i++){
            output->write((const char * )res->buffers[i]->ptr, res->buffers[i]->len);
        }
        // Note that we do not free results
        // We will raise semaphore for to have it next time
        for(int i = 0; i < 4; i++){
            sem_post(&res->sem); // Spark can use as many cores as he wants
        }
        return 0;        
    }
    
    //std::string resp("LambdaProcessorReadAhead::Run [" + dikeProcessorConfig["ID"] + "]");
    //std::cout << resp << " " << rowGroupIndex << std::endl;    
    
    LambdaResult * res = lambdaResultVector->resultVector[rowGroupIndex];
    sem_wait(&res->sem); // This will change when we will support NCP
    lambdaResultVector->lock.lock();
    if (res->state == LambdaResult::READY) {
        res->state = LambdaResult::DONE;
        lambdaResultVector->lock.unlock();
        //std::cout << "LambdaProcessorReadAhead::Run Sending results for " << rowGroupIndex << std::endl;
        // Send results to output        
        for(int i = 0; i < res->buffers.size(); i++){
            output->write((const char * )res->buffers[i]->ptr, res->buffers[i]->len);
        }
        //std::cout << "LambdaProcessorReadAhead::Run Sending for " << rowGroupIndex << " completed " << std::endl;
        lambdaBufferPool.Free(res->buffers);
    } else {
        lambdaResultVector->lock.unlock();
    }

    //std::cout << "Wake up worker " << std::endl;
    sem_post(&lambdaResultVector->sem);

    return 0;
}

void LambdaProcessorReadAhead::Worker(LambdaProcessor * lambdaProcessor)
{    
    int result_index;
    do {
        // wait on lambdaResultVector->sem
        sem_wait(&lambdaResultVector->sem);
        if(done) {
            continue;
        }
        // Lock lambdaResultVector
        lambdaResultVector->lock.lock();
        // Find next index to operate
        result_index = -1;
        for(int i = 0; i < rowGroupCount && result_index == -1; i++){
            LambdaResult * res = lambdaResultVector->resultVector[i];
            if (res->state == LambdaResult::EMPTY){
                res->state = LambdaResult::PENDING;
                result_index = i;
            }
        }
        lambdaResultVector->lock.unlock();
        if(result_index >= 0){
            //std::cout << "LambdaProcessorReadAhead::Worker Run on " << result_index << std::endl;
            // Crank lambdaProcessor
            LambdaResultOutput output = LambdaResultOutput(lambdaResultVector->resultVector[result_index]);
            lambdaProcessor->Run(result_index, &output);
            lambdaResultVector->lock.lock();
            lambdaResultVector->resultVector[result_index]->state = LambdaResult::READY;
            lambdaResultVector->lock.unlock();
            sem_post(&lambdaResultVector->resultVector[result_index]->sem);
            //std::cout << "LambdaProcessorReadAhead::Worker Run on " << result_index << " Done " << std::endl;
        }
    } while(result_index >= 0 && ! done); // Repeat 

    lambdaProcessor->Finish();

    // I am not sure this is a godd place to call destructor
    delete lambdaProcessor;

    //std::cout << "LambdaProcessorReadAhead::Worker Done" << std::endl;
}
