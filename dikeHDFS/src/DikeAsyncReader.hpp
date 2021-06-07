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

#include "DikeUtil.hpp"
#include "DikeBuffer.hpp"
#include "DikeIO.hpp"

class DikeRecord {
    public:
    enum{
        FIELED_SIZE  = 1024,
        MAX_COLUMNS = 128
    };
    int nCol;
    uint8_t * fields[MAX_COLUMNS];
    uint8_t * fieldMemory[MAX_COLUMNS];
    int len[MAX_COLUMNS];    

    DikeRecord(int col) {
        nCol = col;
        uint8_t * buf = (uint8_t *)malloc(FIELED_SIZE * MAX_COLUMNS);
        for(int i = 0; i < nCol; i++) {
            fields[i] = 0;
            fieldMemory[i] = buf + i * FIELED_SIZE;
            len[i] = 0;            
        }
    }
    ~DikeRecord(){
        free(fieldMemory[0]);
    }
};

class DikeAsyncReader {
    public:
    DikeRecord * record = NULL; /* Single record */
    uint64_t blockSize = 0; /* Limit reads to one HDFS block */
    uint64_t blockOffset = 0; /* If not zero we need to seek for record */   
 
    virtual int initRecord(int nCol) = 0;    
    virtual int seekRecord() = 0;
    virtual int getColumnCount() = 0;
    virtual int readRecord() = 0;
    virtual std::thread startWorker() = 0;
    virtual void Worker() = 0;
};

#endif /* DIKE_ASYNC_READER_HPP */