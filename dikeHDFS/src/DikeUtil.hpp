#ifndef DIKE_UTIL_HPP
#define DIKE_UTIL_HPP

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

class DikeUtil{
    public:
    DikeUtil(){ }
    ~DikeUtil(){ }

    std::string Now(){
        std::ostringstream now;
        
        time_t curr_time;
	    curr_time = time(NULL);
	    tm *tm_local = localtime(&curr_time);
        now << std::to_string(tm_local->tm_year - 100) << "/";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_mon + 1) << "/";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_mday) << " ";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_hour) << ":";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_min)  << ":";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_sec);         
        return now.str();
    }
    
    std::string Reset() {
        std::string Reset("\033[0m");
        return Reset;
    }
    std::string Red() {
        std::string Red("\033[0;31m"); 
        return Red;
     }
    std::string Green() {
        std::string Green("\033[0;32m");
        return Green;
    }
    std::string Yellow() {
        std::string Yellow("\033[0;33m");
        return Yellow;
    }
    std::string Blue() {
        std::string Blue("\033[0;34m");
        return Blue;
    }
    std::string Purple() {
        std::string Purple("\033[0;35m");
        return Purple;
    } 
};

class DikeBuffer{
    public:
    uint8_t *startPtr;
    uint8_t *posPtr;
    uint8_t *endPtr;
    DikeBuffer(int size){
        startPtr = (uint8_t *)malloc(size);
        //std::cout << "Allocating "  << size << " bytes" << std::endl;
        posPtr = startPtr;
        endPtr = startPtr + size;
    }

    ~DikeBuffer(){
        //std::cout << "~DikeBuffer" << std::endl;
        free(startPtr);
        startPtr = 0;
    }

    int write(const char * data, char delim){
        uint8_t * d = (uint8_t *)data;
        uint8_t * p = posPtr;
        int rc;
        while(*d != 0 && p < endPtr && *d != delim){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr){ // out of space new buffer needed
            return 0;
        }

        if(*d == 0){ // end of data
            *p = delim;
            p++;
            rc = p - posPtr;
            posPtr = p;
            return rc;
        }

        if(*d != delim){ // sanity check
            std::cout << "Something is very wrong there" << std::endl;
            return 0;
        }

        // We need to wrap delimiter in quotations
        d = (uint8_t *)data;
        p = posPtr;
        *p = '"';
        p++;
        while(*d != 0 && p < endPtr){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr-2){ // out of space new buffer needed
            return 0;
        }
        *p = '"';
        p++;
        *p = delim;
        p++;
        rc = p - posPtr;
        posPtr = p;
        return rc;
    }

    int write(const char * data, char delim, char term){
        uint8_t * d = (uint8_t *)data;
        uint8_t * p = posPtr;
        int rc;
        while(*d != 0 && p < endPtr && *d != delim){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr){ // out of space new buffer needed
            return 0;
        }

        if(*d == 0){ // end of data
            *p = term;
            p++;
            rc = p - posPtr;
            posPtr = p;
            return rc;
        }

        if(*d != delim){ // sanity check
            std::cout << "Something is very wrong there" << std::endl;
            return 0;
        }

        // We need to wrap delimiter in quotations
        d = (uint8_t *)data;
        p = posPtr;
        *p = '"';
        p++;
        while(*d != 0 && p < endPtr){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr-2){ // out of space new buffer needed
            return 0;
        }
        *p = '"';
        p++;
        *p = term;
        p++;
        rc = p - posPtr;
        posPtr = p;
        return rc;
    }

    int write(char term){
        uint8_t * p = posPtr;
        if( p < endPtr){
            *p = term;
            p++;
            posPtr = p;
            return 1;
        } else {
            return 0;
        }
    }

    void reset(){
        assert(posPtr <= endPtr);
        posPtr = startPtr;
    }

    std::streamsize getSize(){
        assert(posPtr <= endPtr);
        int rc = posPtr - startPtr;
        assert(rc > 0);
        return std::streamsize(rc);
    }
};
class DikeAyncWriter{
    public:
    enum{
        QUEUE_SIZE  = 4,
        BUFFER_SIZE = (8 << 10)
    };
    std::ostream * outStream;
    std::queue<DikeBuffer * > work_q;
    std::queue<DikeBuffer * > free_q;
    std::mutex q_lock;
    sem_t work_sem;
    sem_t free_sem;    
    std::thread workerThread;
    bool isRunning;
    DikeBuffer * buffer = NULL;

    DikeAyncWriter(std::ostream * outStream){
        this->outStream = outStream;
        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);        
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(BUFFER_SIZE);
            free_q.push(b);
        }
        buffer = NULL;
        buffer = getBuffer();
        isRunning = true;
        workerThread = startWorker();
    }

    ~DikeAyncWriter(){
        isRunning = false;
        //std::cout << "~DikeAyncWriter" << std::endl;
        sem_post(&work_sem);
        workerThread.join();
        sem_destroy(&work_sem);

        delete buffer;
        DikeBuffer * b;
        while(!free_q.empty()){
            b = free_q.front();
            free_q.pop();
            delete b;
        }
    }    

    DikeBuffer * getBuffer(){
        if(buffer != NULL) {
            q_lock.lock();
            work_q.push(buffer);
            buffer = NULL;
            q_lock.unlock();
            sem_post(&work_sem);
        }

        sem_wait(&free_sem);
        q_lock.lock();
        DikeBuffer * b = free_q.front();
        free_q.pop();
        q_lock.unlock();
        return b;
    }

    void write(const char * data, char delim){
        if(buffer->write(data, delim)){
            return;
        }
        buffer = getBuffer();
        buffer->write(data, delim);        
    }

    void write(const char * data, char delim, char term){
        if(buffer->write(data, delim, term)){
            return;
        }
        buffer = getBuffer();
        buffer->write(data, delim, term);        
    }

    void write(char term){
        if(buffer->write(term)){
            return;
        }
        buffer = getBuffer();
        buffer->write(term);        
    }

    void flush(){
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
        outStream->flush();
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
                //std::cout << "Exiting worker thread" << std::endl;
                return;                
            }
            DikeBuffer * b = work_q.front();             
            work_q.pop();
            q_lock.unlock();
            outStream->write((const char *)b->startPtr, b->getSize());
            outStream->flush();
            b->reset();
            q_lock.lock();
            free_q.push(b);            
            q_lock.unlock();
            sem_post(&free_sem);
        }
    }
};

#endif /* DIKE_UTIL_HPP */