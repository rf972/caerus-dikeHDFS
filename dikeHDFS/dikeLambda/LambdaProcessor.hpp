#ifndef LAMBDA_PROCESSOR
#define LAMBDA_PROCESSOR

#include <queue>
#include <mutex>
#include <semaphore.h>
#include <vector>
#include <atomic>

#include "DikeProcessor.hpp"
#include "LambdaNode.hpp"

class LambdaBuffer {
    public:
    uint8_t * ptr = NULL;
    uint32_t len = 0; // data len
    uint32_t size = 0; // memory size

    static std::atomic<int> allocCount;
    static std::atomic<int> freeCount;

    LambdaBuffer(){
        allocCount++;
    }

    void resize(uint32_t new_size) {
        if(size < new_size) {
            if(ptr != NULL){
                delete [] ptr;
            }
            ptr = new uint8_t [new_size];
            if(ptr == NULL){
                std::cout << "Failed to allocate memory" << std::endl;
            }
            size = new_size;
        }
    }

    void copy(const char * from, uint32_t len) {
        memcpy(ptr, from, len);
        this->len = len;
    }

    ~LambdaBuffer() {
        freeCount++;
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
            if(buffer == NULL){
                std::cout << "Can't allocate LambdaBuffer "  << std::endl;
            }
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
            //queue.push(buffers[i]);
            delete buffers[i];
            buffers[i] = NULL;
        }
        lock.unlock();
        buffers.clear();
    }
};

extern LambdaBufferPool lambdaBufferPool;

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
    ~LambdaResult() {
        sem_destroy(&sem);
        lambdaBufferPool.Free(buffers);
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
    int totalResults = 0;
    LambdaProcessor(){}
    virtual ~LambdaProcessor(){}

    virtual void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    virtual int Run(int rowGroupIndex, DikeIO * output);
    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output){ return 0;};
    virtual void Finish();
};

class LambdaProcessorReadAhead : public LambdaProcessor{    
    public:
    LambdaResultVector * lambdaResultVector  = NULL;
    std::vector<std::thread> workerThread;
    bool done = false;

    LambdaProcessorReadAhead(){};
    virtual ~LambdaProcessorReadAhead() {
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

    virtual void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    void Worker(LambdaProcessor * lambdaProcessor);
};

class LambdaProcessorTotal : public LambdaProcessor{    
    public:
    LambdaResultVector * lambdaResultVector  = NULL;
    std::vector<std::thread> workerThread;
    bool done = false;
    int nWorkers = 0;

    std::vector<std::vector<lambda::Node *>> nodeTree;

    LambdaProcessorTotal(){};
    virtual ~LambdaProcessorTotal();

    virtual void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    void Worker(int workerID);
    void TrunkWorker();

    void CreateGraphs(DikeProcessorConfig & dikeProcessorConfig);
};

class LambdaProcessorFactory {    
    public:
    int verbose = 0;
    static std::map<std::string, LambdaProcessor *> processorMap;
    LambdaProcessorFactory(){};

    void Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
};


class LambdaResultOutput : public DikeIO {
    public:
    LambdaResult * result = NULL;    

    LambdaResultOutput(LambdaResult * result) {
        this->result = result;
    }

    ~LambdaResultOutput(){ }
    virtual int write(const char * buf, uint32_t size) override {
        //std::cout << "LambdaResultOutput::write " << size << " bytes" << std::endl;
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

#endif /* LAMBDA_PROCESSOR */