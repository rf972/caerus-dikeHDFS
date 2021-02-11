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

class DikeAyncWriter{
    public:
    enum{
        QUEUE_SIZE  = 4,
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

    DikeAyncWriter(DikeIO * output){
        this->output = output;

        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);        
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(BUFFER_SIZE);
            free_q.push(b);
        }
        buffer = NULL;
        buffer = getBuffer();
        isRunning = true;
        //workerThread = startWorker();
    }

    ~DikeAyncWriter(){
        isRunning = false;
        //std::cout << "~DikeAyncWriter" << std::endl;
        //sem_post(&work_sem);
        //workerThread.join();
        sem_destroy(&work_sem);

        delete buffer;
        DikeBuffer * b;
        while(!free_q.empty()){
            b = free_q.front();
            free_q.pop();
            delete b;
        }
        std::cout << "~DikeAyncWriter Push count: " << pushCount << " Empty count: " << emptyCount;
        std::cout << " Record count: " << recordCount << std::endl;
    }    

    void close(){
        flush();
        isRunning = false;
        sem_post(&work_sem);
    }

    DikeBuffer * getBuffer(){
        if(buffer != NULL) {
            buffer->validate();

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

    int write(const char * data, char delim){
        if(!isRunning){
            return 0;
        }
        int rc = buffer->write(data, delim);        
        if(rc){
            return rc;
        }
        
        buffer = getBuffer();
        rc = buffer->write(data, delim);
        return rc;
    }

    int write(const char **res, int data_count, char delim, char term, int total_bytes)
    {
        if(!isRunning){
            return 0;
        }        

        // We need to terminate each field so we do (+ data_count)
        int rc = buffer->write(res, data_count, delim, term, total_bytes + data_count);        
        if(rc){           
            return rc;
        }
        buffer = getBuffer();
        rc = buffer->write(res, data_count, delim, term, total_bytes + data_count);

        if(!rc){           
            std::cout << "DikeAyncWriter::write failed" << std::endl;
            std::cout << "total_bytes " << total_bytes << std::endl;
            std::cout << "buffer size " << buffer->getSize() << std::endl;
        }

        return rc;
    }

    int write(const char * data, char delim, char term){
        if(!isRunning){
            return 0;
        }                

        int rc = buffer->write(data, delim, term);        
        if(rc){           
            return rc;
        }
        buffer = getBuffer();
        rc = buffer->write(data, delim, term);        
        return rc;
    }

    int write(char term){
        if(!isRunning){
            return 0;
        }        

        int rc = buffer->write(term);       
        if(rc){
            return rc;
        }
        buffer = getBuffer();
        rc = buffer->write(term);        
        return rc;
    }

    void flush(){
        if(!isRunning){
            return;
        }

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

        //std::cout << "Flush completed" << std::endl;        
    }

    std::thread startWorker() {
        return std::thread([=] { Worker(); });
    }

    void Worker() {
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
                if(len > 0 && isRunning){                    
                    //std::cout << recordCount << " len " << len << std::endl; 
                    //std::cout << std::string((char*)&b->startPtr[n], len) << std::endl; 
                    recordCount++;
                    n = output->write((char*)b->startPtr, len);
                    if(n < len) {
                        std::cout << "DikeAyncWriter Client disconnected " << std::endl;                    
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