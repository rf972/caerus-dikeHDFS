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

class LambdaResultOutput : public DikeIO {
    public:
    LambdaResult * result = NULL;
    static LambdaBufferPool lambdaBufferPool;

    LambdaResultOutput(LambdaResult * result) {
        this->result = result;
    }

    ~LambdaResultOutput(){ }
    virtual int write(const char * buf, uint32_t size) override {
        // Allocate buffer
        LambdaBuffer * buffer = lambdaBufferPool.Allocate(size);
        // Copy memory        
        buffer->copy(buf, size);
        // Push buffer to result
        result->buffers.push_back(buffer);
        return size;
    }
    virtual int read(char * buf, uint32_t size){return -1;};    
};

void LambdaProcessorReadAhead::Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    std::string resp("LambdaProcessorReadAhead::Init " + dikeProcessorConfig["ID"] );
    std::cout << resp << std::endl;

    output->write(resp.c_str(), resp.length());

    LambdaProcessor * lambdaProcessor = new LambdaProcessor;
    dikeProcessorConfig["Configuration.RowGroupIndex"] = "0";
    lambdaProcessor->Init(dikeProcessorConfig, output);
    rowGroupCount = lambdaProcessor->rowGroupCount;

    lambdaResultVector = new LambdaResultVector(rowGroupCount, 2);

    std::thread workerThread = std::thread([=] { Worker(lambdaProcessor); });
}

// This will simply send back results
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
        LambdaResultOutput::lambdaBufferPool.Free(res->buffers);
    } else {
        lambdaResultVector->lock.unlock();
    }

    return 0;
}

void LambdaProcessorReadAhead::Worker(LambdaProcessor * lambdaProcessor)
{    
    int result_index = -1;
    // wait on lambdaResultVector->sem
    sem_wait(&lambdaResultVector->sem);
    // Lock lambdaResultVector
    lambdaResultVector->lock.lock();
    // Find next index to operate
    for(int i = 0; i < rowGroupCount; i++){
        LambdaResult * res = lambdaResultVector->resultVector[i];
        if (res->state == LambdaResult::EMPTY){
            res->state = LambdaResult::PENDING;
            result_index = i;
        }
    }
    lambdaResultVector->lock.unlock();
    if(result_index < 0){
        // We have nothing to do
        std::cout << "LambdaProcessorReadAhead::Worker Done" << std::endl;
        return;
    }
    // Crank lambdaProcessor
    // Repeat 
}

LambdaBufferPool LambdaResultOutput::lambdaBufferPool;