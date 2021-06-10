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

int StreamReaderInit(sqlite3 *db, DikeAsyncReader * dikeAsyncReader);

#endif /* STREAM_READER_H */