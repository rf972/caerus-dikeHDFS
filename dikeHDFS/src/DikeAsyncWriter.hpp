#ifndef DIKE_ASYNC_WRITER_HPP
#define DIKE_ASYNC_WRITER_HPP

#include <iostream>
#include <chrono> 
#include <ctime>
#include <string>
#include <sstream> 
#include <iomanip>
#include <thread>
#include <queue>
#include <mutex>
#include <cassert>
#include <semaphore.h>
#include <unistd.h>

#include "DikeUtil.hpp"
#include "DikeIO.hpp"
#include "DikeBuffer.hpp"
      
class DikeAsyncWriter {
    public:
    enum{
        QUEUE_SIZE  = 8,
        BUFFER_SIZE = (256 << 10)
    };

    DikeIO * output;
    std::queue<DikeBuffer * > work_q;
    std::queue<DikeBuffer * > free_q;
    std::mutex q_lock;
    sem_t work_sem;
    sem_t free_sem;    
    std::thread workerThread;
    bool isRunning;
    DikeBuffer * buffer = NULL;
    int pushCount = 0;
    int emptyCount = 0;
    int recordCount = 0;

    DikeAsyncWriter(DikeIO * output) {
        std::cout << "DikeAsyncWriter::DikeAsyncWriter " << std::endl;
        this->output = output;
        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);        
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(BUFFER_SIZE);
            b->id = i;
            free_q.push(b);
        }
        buffer = NULL;
        buffer = getBuffer();
        isRunning = true;
        //workerThread = startWorker();
    }

    virtual ~DikeAsyncWriter(){
        //std::cout << "~DikeAsyncWriter " << std::endl;
        isRunning = false;
        sem_destroy(&work_sem);

        delete buffer;
        DikeBuffer * b;
        while(!free_q.empty()){
            b = free_q.front();
            free_q.pop();
            delete b;
        }
    }    

    virtual void close(){
        //std::cout << "DikeAsyncWriter::close " << std::endl;
        flush();
        isRunning = false;
        sem_post(&work_sem);
    }

    DikeBuffer * getBuffer() {
        if(buffer != NULL) {
            //std::cout << "DikeAsyncWriter::getBuffer " << buffer->getSize() << std::endl;
            q_lock.lock();
            pushCount ++;
            if(work_q.empty()){
                emptyCount++;
            }
            work_q.push(buffer);
            buffer = NULL;
            q_lock.unlock();
            sem_post(&work_sem);
        }

        sem_wait(&free_sem);
        q_lock.lock();
        assert(!free_q.empty());
        DikeBuffer * b = free_q.front();
        free_q.pop();
        q_lock.unlock();
        return b;
    }

    //virtual int write(const char **res, int data_count, char delim, char term, int total_bytes) = 0;
    //virtual int write(char term) = 0;
    virtual int write(void *res) {
        std::cout << "DikeAsyncWriter::write Not implemented" << std::endl;
        return 0;
    };

    void flush(){
        if(!isRunning){
            return;
        }
        //std::cout << "DikeAsyncWriter::flush " << std::endl;

        buffer = getBuffer();
        /* Busy wait for work_q to be empty */
        bool isEmpty = false;
        while(!isEmpty){
            q_lock.lock();
            isEmpty = work_q.empty();
            q_lock.unlock();
            if(!isEmpty){
                usleep(100);
            }
        }  
    }

    std::thread startWorker() {
        std::cout << "DikeAsyncWriter::startWorker " << std::endl;
        return std::thread([this] { this->Worker(); });
    }

    void Worker() {
        std::cout << "DikeAsyncWriter::Worker " << std::endl;
        while(1){
            sem_wait(&work_sem);
            q_lock.lock();
            if(work_q.empty()){
                q_lock.unlock();
                isRunning = false;
                //std::cout << "Exiting worker thread" << std::endl;
                return;                
            }
            DikeBuffer * b = work_q.front();             
            work_q.pop();
            q_lock.unlock();
            if(output) {
                int len = b->getSize();
                int n = 0;
                if(len > 0 && isRunning) {                    
                    //std::cout << "DikeAsyncWriter:Worker bufferId " << b->id << " len " << len << std::endl; 
                    //b->validateBeforeWrite();
                    recordCount++;
                    n = output->write((char*)b->startPtr, len);
                    if(n < len) {
                        std::cout << "DikeAsyncWriter Client disconnected " << std::endl;                    
                        isRunning = false;                   
                    }
                }
            }

            b->reset();
            q_lock.lock();
            free_q.push(b);            
            q_lock.unlock();
            sem_post(&free_sem);
        }
    }
};

#endif /* DIKE_ASYNC_WRITER_HPP */