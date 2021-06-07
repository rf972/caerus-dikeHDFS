#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <sqlite3.h>
#include <iostream>
#include <string.h>

#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/HTTPSession.h"

#include "DikeAsyncReader.hpp"

typedef enum header_info_e {
    HEADER_INFO_NONE,
    HEADER_INFO_IGNORE,
    HEADER_INFO_USE
}header_info_t;

/* This is a copy from sqlite3.c file */
typedef enum sqlite_aff_e {
    SQLITE_AFF_NONE     = 0x40,  /* '@' */
    SQLITE_AFF_BLOB     = 0x41,  /* 'A' */
    SQLITE_AFF_TEXT     = 0x42,  /* 'B' */
    SQLITE_AFF_NUMERIC  = 0x43,  /* 'C' */
    SQLITE_AFF_INTEGER  = 0x44,  /* 'D' */
    SQLITE_AFF_REAL     = 0x45,  /* 'E' */
} sqlite_aff_t;

static header_info_t GetHeaderInfo(std::string & headerInfo) 
{
    std::string str = headerInfo;
    header_info_t info = HEADER_INFO_NONE;
    // Convert header info string to uppercase
    for (auto & c: str) c = toupper(c);

    if(!str.compare(0,4,"NONE")){
        info = HEADER_INFO_NONE;
    } else if(!str.compare(0,6,"IGNORE")){
        info = HEADER_INFO_IGNORE;
    } else if(!str.compare(0,3,"USE")) {
        info = HEADER_INFO_USE;
    }
    return info;
}

struct StreamReaderParam {
    DikeAsyncReader * reader;
    std::string name;
    std::string schema;
    header_info_t headerInfo;
};

int StreamReaderInit(sqlite3 *db, StreamReaderParam * param);

#endif /* STREAM_READER_H */