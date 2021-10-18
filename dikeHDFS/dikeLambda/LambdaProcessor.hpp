#ifndef LAMBDA_PROCESSOR
#define LAMBDA_PROCESSOR

#include <queue>
#include <mutex>
#include <semaphore.h>
#include <vector>

#include "DikeProcessor.hpp"
#include "LambdaNode.hpp"

class LambdaBuffer {
    public:
    uint8_t * ptr;
    uint32_t len; // data len
    uint32_t size; // memory size
};

class LambdaResult {
    public:
    enum {
        EMPTY,
        PENDING,
        READY,
        DONE
    };        
    std::vector<LambdaBuffer *> buffers;
    std::mutex buffersLock;
    int state;
    LambdaResult(int state) {
        this->state = state;
    }
};

class LambdaResultVector {
    public:
    std::vector<LambdaResult *> resultVector;
    std::mutex lock;
    sem_t sem;
    LambdaResultVector(int resultSize, int batchSize) {
        sem_init(&sem, 0, batchSize);
        resultVector.resize(resultSize, NULL);

        for(int i = 0; i < resultSize; i++) {
            LambdaResult * r = new LambdaResult(LambdaResult::EMPTY);
            resultVector[i] = r;
        }
    }
    ~LambdaResultVector() {
        for(int i = 0; i < resultVector.size(); i++) {
            delete resultVector[i];
        }
    }
};

class LambdaProcessor{    
    public:
    int verbose = 0;
    std::vector<lambda::Node *> nodeVector;
    int rowGroupCount = 0;
    LambdaProcessor(){}

    void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);    
};

class LambdaProcessorReadAhead {    
    public:
    int verbose = 0;
    int rowGroupCount = 0;
    LambdaResultVector * lambdaResultVector  = NULL;
    LambdaProcessorReadAhead(){};
    ~LambdaProcessorReadAhead() {
        if(lambdaResultVector){
            delete lambdaResultVector;
        }
    }

    void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    void Worker(LambdaProcessor * lambdaProcessor);
};

class LambdaProcessorFactory {    
    public:
    int verbose = 0;
    static std::map<std::string, LambdaProcessorReadAhead *> processorMap;
    LambdaProcessorFactory(){};

    void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
};


#endif /* LAMBDA_PROCESSOR */