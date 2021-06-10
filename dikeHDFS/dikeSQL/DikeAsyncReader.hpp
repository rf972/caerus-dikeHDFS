#ifndef DIKE_ASYNC_READER_HPP
#define DIKE_ASYNC_READER_HPP

#include <thread>
#include <string.h>

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
    virtual int initRecord(int nCol) = 0;        
    virtual int getColumnCount() = 0;
    virtual int readRecord() = 0;
};

#endif /* DIKE_ASYNC_READER_HPP */