#ifndef DIKE_ASYNC_READER_HPP
#define DIKE_ASYNC_READER_HPP

#include <thread>
#include <string.h>

/* This is a copy from sqlite3.c file */
typedef enum sqlite_aff_e {
    SQLITE_AFF_INVALID      = 0x00,  

    SQLITE_AFF_NONE         = 0x40,  /* '@' */
    SQLITE_AFF_BLOB         = 0x41,  /* 'A' */
    SQLITE_AFF_TEXT         = 0x42,  /* 'B' */
    SQLITE_AFF_NUMERIC      = 0x43,  /* 'C' */
    SQLITE_AFF_INTEGER      = 0x44,  /* 'D' */
    SQLITE_AFF_REAL         = 0x45,  /* 'E' */

    // Additinal qualifiers
    SQLITE_AFF_TEXT_TERM    = 0x54,  /* 'T' - NULL terminated text */    
} sqlite_aff_t;

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
    //sqlite_aff_t affinity[MAX_COLUMNS];
    //int accessMap[MAX_COLUMNS];
    
    DikeRecord(int col) {
        nCol = col;
        uint8_t * buf = (uint8_t *)malloc(FIELED_SIZE * MAX_COLUMNS);
        for(int i = 0; i < nCol; i++) {
            fields[i] = 0;
            fieldMemory[i] = buf + i * FIELED_SIZE;
            len[i] = 0;
            //accessMap[i] = 0;
        }
    }
    ~DikeRecord(){
        free(fieldMemory[0]);
    }
};

class DikeAsyncReader {
    public:    
    virtual int getColumnCount() = 0;
    virtual int readRecord() = 0;
    virtual const std::string &  getSchema() = 0;
    virtual int getColumnValue(int col, void ** value, int * len, sqlite_aff_t * affinity) = 0;

    DikeRecord * record = NULL; /* Single record */
};

#endif /* DIKE_ASYNC_READER_HPP */