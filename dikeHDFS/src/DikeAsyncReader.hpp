#ifndef DIKE_ASYNC_READER_HPP
#define DIKE_ASYNC_READER_HPP

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

#include <string.h>

#include "Poco/Net/HTTPSession.h"

#include "DikeUtil.hpp"
#include "DikeBuffer.hpp"

class DikeRecord {
    public:
    enum{
        FILED_SIZE  = 1024,
        MAX_COLUMNS = 128
    };
    int nCol;
    uint8_t * fields[MAX_COLUMNS];
    uint8_t * fieldMemory[MAX_COLUMNS];
    int len[MAX_COLUMNS];    

    DikeRecord(int col) {
        nCol = col;
        uint8_t * buf = (uint8_t *)malloc(FILED_SIZE * MAX_COLUMNS);
        for(int i = 0; i < nCol; i++) {
            fields[i] = 0;
            fieldMemory[i] = buf + i * FILED_SIZE;
            len[i] = 0;            
        }
    }
    ~DikeRecord(){
        free(fieldMemory[0]);
    }
};

class DikeAsyncReader{
    public:
    enum{
        QUEUE_SIZE  = 4,
        BUFFER_SIZE = (256 << 10)
    };
    std::istream * inStream = NULL;   
    Poco::Net::HTTPSession * inSession;

    uint64_t blockSize = 0; /* Limit reads to one HDFS block */
    uint64_t blockOffset = 0; /* If not zero we need to seek for record */
    int bytesRead = 0; /* How many bytes did we read so far */
    int fDelim = '|'; /* Field delimiter */
    int rDelim = '\n'; /* Record Delimiter */

    DikeRecord * record = NULL; /* Single record */

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
    uint64_t recordCount = 0;

    DikeAsyncReader(std::istream * inStream){
        this->inStream = inStream;        
        this->inSession = NULL;
        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);        
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(BUFFER_SIZE);
            free_q.push(b);
        }
        buffer = NULL;        
        isRunning = true;
        workerThread = startWorker(); // This will start reading immediatelly
        buffer = getBuffer();
    }

    DikeAsyncReader(Poco::Net::HTTPSession * inSession){
        this->inStream = NULL;        
        this->inSession = inSession;
        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);        
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(BUFFER_SIZE);
            free_q.push(b);
        }
        buffer = NULL;        
        isRunning = true;
        workerThread = startWorker();
        buffer = getBuffer();
    }

    ~DikeAsyncReader(){
        isRunning = false;
        //std::cout << "~DikeAsyncReader" << std::endl;
        sem_post(&free_sem);
        workerThread.join();
        sem_destroy(&work_sem);
        sem_destroy(&free_sem);

        delete buffer;
        DikeBuffer * b;
        while(!free_q.empty()){
            b = free_q.front();
            free_q.pop();
            delete b;
        }
        while(!work_q.empty()){
            b = work_q.front();
            work_q.pop();
            delete b;
        }
        if(record){
            delete record;
        }
        std::cout << "~DikeAsyncReader Push count: " << pushCount << " Empty count: " << emptyCount << std::endl;        
    }    

    int initRecord(int nCol){
        // At this time blockOffset should be set
        if(blockOffset > 0){
            seekRecord();
        }
        record = new DikeRecord(nCol);
        return (record != NULL);
    }

    bool isEOF() {
        if(blockSize > 0 && bytesRead > blockSize) {
            return true;
        }
        return false;
    }
    
    int seekRecord() {
        uint8_t * posPtr = buffer->posPtr;        
        
        while(*posPtr != rDelim && 
              posPtr < buffer->endPtr && 
              *posPtr != 0) 
        {
            posPtr++;                       
        }

        if(*posPtr == rDelim){
            bytesRead += posPtr - buffer->posPtr + 1;
            buffer->posPtr = posPtr + 1; // Skiping delimiter            
            return 0;
        }

        std::cout << "DikeAsyncReader failed seek " << std::endl;
        return 1;
    }
    
    int readRecord() {
        if(isEOF()){
            std::cout << "DikeAsyncReader EOF at " << bytesRead << std::endl;
            return 1;
        }

        recordCount++;
        for(int i = 0; i < record->nCol; i++) {
            record->fields[i] = NULL;
            if(readField(i)){
                //std::cout << "DikeAsyncReader failed to read at pos " << i << std::endl;
                std::cout << "DikeAsyncReader EOF at " << bytesRead << std::endl;
                return 1;
            }
#if _DEBUG
            assert(NULL == strchr((char *)record->fields[i], fDelim));
            std::size_t found = std::string((const char*)record->fields[i]).find('|');
            assert(std::string::npos == found);
#endif            
        }
        
        if(0 && bytesRead < 10240){
            for(int i = 0; i < record->nCol; i++) {
                std::cout << std::string((char*)record->fields[i]) << ",";
            }
            std::cout << std::endl;
        }

        
        return 0;
    }
    int readField(int pos) {       
        if(buffer->posPtr >= buffer->endPtr) {
            buffer = getBuffer();
        }

        if(pos == 0 && *buffer->posPtr == rDelim){ // Skip CR if it is first
            buffer->posPtr++;
            bytesRead += 1;
            if(buffer->posPtr >= buffer->endPtr) {
                buffer = getBuffer();
            }
        }

        int count = 0;
        uint8_t * posPtr = buffer->posPtr;
        uint8_t * fieldPtr = record->fieldMemory[pos];
        record->fields[pos] = fieldPtr;
        // Copy first part
        posPtr = buffer->posPtr;

        while( *posPtr != fDelim && *posPtr != rDelim && *posPtr != 0) {
            *fieldPtr = *posPtr;
            posPtr++;
            fieldPtr++;
            count++;
            if(posPtr >= buffer->endPtr) {
                buffer = getBuffer();
                posPtr = buffer->posPtr;
            }
        }

        if(*posPtr == fDelim || *posPtr == rDelim) {                
            //record->len[pos] = count;
            bytesRead += count + 1;
            *fieldPtr = 0;
            *posPtr = 0;
            buffer->posPtr = posPtr + 1; // Skiping delimiter                                
            return 0;
        }
        
        return 1;        
    }

#if 0
    int readField(int pos) {       
        if(buffer->posPtr >= buffer->endPtr) {
            buffer = getBuffer();
        }

        if(pos == 0 && *buffer->posPtr == rDelim){ // Skip CR if it is first
            buffer->posPtr++;
            bytesRead += 1;
            if(buffer->posPtr >= buffer->endPtr) {
                buffer = getBuffer();
            }
        }

        uint8_t * posPtr = buffer->posPtr;

        while(*posPtr != fDelim && 
              *posPtr != rDelim && 
              posPtr < buffer->endPtr && 
              *posPtr != 0) 
        {
            posPtr++;                       
        }

        if(*posPtr == fDelim || *posPtr == rDelim) {
            record->fields[pos] = buffer->posPtr;
            //record->len[pos] = posPtr - buffer->posPtr;
            bytesRead += posPtr - buffer->posPtr + 1;
            buffer->posPtr = posPtr + 1; // Skiping delimiter            
            *posPtr = 0;            
            return 0;
        }

        if(posPtr >= buffer->endPtr) { // Use internal memory
            int count = 0;
            uint8_t * fieldPtr = record->fieldMemory[pos];
            record->fields[pos] = fieldPtr;
            // Copy first part
            posPtr = buffer->posPtr;
            while( posPtr < buffer->endPtr && *posPtr != 0) {
                *fieldPtr = *posPtr;
                posPtr++;
                fieldPtr++;
                count++;
            }
            // Get new buffer
            buffer = getBuffer();
            posPtr = buffer->posPtr;
            // Copy second part
            while(*posPtr != fDelim && 
                *posPtr != rDelim && 
                posPtr < buffer->endPtr && 
                *posPtr != 0) 
            {
                *fieldPtr = *posPtr;
                posPtr++;
                fieldPtr++;
                count++;
            }

            if(*posPtr == fDelim || *posPtr == rDelim) {                
                //record->len[pos] = count;
                bytesRead += count + 1;
                *fieldPtr = 0;
                buffer->posPtr = posPtr + 1; // Skiping delimiter                                
                return 0;
            }
        }  // Use internal memory
        return 1;
    }
#endif
    DikeBuffer * getBuffer(){
        if(buffer != NULL) {
            q_lock.lock();
            pushCount ++; // We processed this buffer
            if(free_q.empty()){
                emptyCount++; // We reading faster than processing
            }
            free_q.push(buffer);
            buffer = NULL;
            q_lock.unlock();
            sem_post(&free_sem);
        }

        sem_wait(&work_sem);
        q_lock.lock();
        DikeBuffer * b = work_q.front();
        work_q.pop();
        q_lock.unlock();
        return b;
    }

    std::thread startWorker() {
        return std::thread([=] { Worker(); });
    }

    void Worker() {
        pthread_t thread_id = pthread_self();
        pthread_setname_np(thread_id, "DikeAsyncReader::Worker");

        while(1){
            sem_wait(&free_sem);
            if(isEOF()){
                std::cout << "DikeAsyncReader EOF exiting worker thread" << std::endl;
                return;                
            }
            if(!isRunning){
                std::cout << "DikeAsyncReader not running is set exiting " << std::endl;
                return;
            }

            q_lock.lock();
            if(free_q.empty()){
                std::cout << "DikeAsyncReader exiting worker thread" << std::endl;
                q_lock.unlock();
                return;
            }
            DikeBuffer * b = free_q.front();             
            free_q.pop();
            q_lock.unlock();

            b->reset();
            std::size_t n = 0;
            int len = BUFFER_SIZE;
            try {
                if(inStream){
                    while( len > 0 && inStream->good() ) {
                        inStream->read( (char*)&b->startPtr[n], len );
                        int i = inStream->gcount();
                        n += i;
                        len -= i;
                        if(i <= 0){
                            // Graceful shutdown
                            // We may need to set EOF here
                            break;
                        }
                    }
                } 
#if 0                
                else {
                    while( len > 0 ) {
                        int i = inSession->read( (char*)&b->startPtr[n], len);      
                        n += i;
                        len -= i;
                        if(i <= 0){
                            // Graceful shutdown
                            // We may need to set EOF here
                            break;
                        }
                    }
                }
#endif                
            } catch (...) {
                std::cout << "readFromSession: Caught exception " << std::endl;
            }     
            if(n < BUFFER_SIZE){
                memset((char*)&b->startPtr[n], 0, len);
            }
            q_lock.lock();
            work_q.push(b);            
            q_lock.unlock();
            sem_post(&work_sem);
        }
    }
};

#endif /* DIKE_ASYNC_READER_HPP */