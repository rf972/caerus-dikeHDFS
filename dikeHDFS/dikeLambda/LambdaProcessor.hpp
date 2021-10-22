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
    uint8_t * ptr = NULL;
    uint32_t len = 0; // data len
    uint32_t size = 0; // memory size

    LambdaBuffer(){}

    void resize(uint32_t new_size) {
        if(size < new_size) {
            if(ptr != NULL){
                delete [] ptr;
            }
            ptr = new uint8_t [new_size];
            size = new_size;
        }
    }

    void copy(const char * from, uint32_t len) {
        memcpy(ptr, from, len);
        this->len = len;
    }

    ~LambdaBuffer() {
        if(ptr != NULL){
            delete [] ptr;
        }
    }
};

class LambdaBufferPool {
    public:
    std::mutex lock;
    std::queue<LambdaBuffer *> queue;
    LambdaBufferPool(){}
    ~LambdaBufferPool(){}

    LambdaBuffer * Allocate(uint32_t size) {
        LambdaBuffer * buffer = NULL;
        lock.lock();
        if(!queue.empty()){
            buffer = queue.front();
            queue.pop();
        }
        lock.unlock();
        if(buffer == NULL){ // Q was empty
            buffer = new LambdaBuffer;
        }
        buffer->resize(size);
        return buffer;        
    }

    void Free(LambdaBuffer * buffer){
        lock.lock();
        queue.push(buffer);
        lock.unlock();
    }
    void Free(std::vector<LambdaBuffer *> & buffers) {
        lock.lock();
        for(int i = 0; i < buffers.size(); i++){            
            queue.push(buffers[i]);
            buffers[i] = NULL;
        }
        lock.unlock();
        buffers.clear();
    }
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
    int state;
    sem_t sem;
    LambdaResult(int state) {
        this->state = state;
        sem_init(&sem, 0, 0);
    }
    ~LambdaResult();
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
        sem_destroy(&sem);
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
    int Run(int rowGroupIndex, DikeIO * output);
    void Finish();
};

class LambdaProcessorReadAhead {    
    public:
    int verbose = 0;
    int rowGroupCount = 0;
    LambdaResultVector * lambdaResultVector  = NULL;
    std::vector<std::thread> workerThread;
    bool done = false;

    LambdaProcessorReadAhead(){};
    ~LambdaProcessorReadAhead() {
        //std::cout << "~LambdaProcessorReadAhead()" << std::endl;
        done = true;
        // Do it for each worker
        for(int i = 0; i < workerThread.size(); i++) {
            sem_post(&lambdaResultVector->sem);
        }

        for(int i = 0; i < workerThread.size(); i++) {
            workerThread[i].join();
        }

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