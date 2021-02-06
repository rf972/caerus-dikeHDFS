#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <sqlite3.h>
#include <iostream>
#include <string.h>

#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/HTTPSession.h"

#include "DikeAsyncReader.hpp"

struct StreamReaderParam {
    DikeAsyncReader * reader;
    std::string name;
    std::string schema;
};

int StreamReaderInit(sqlite3 *db, StreamReaderParam * param);

#endif /* STREAM_READER_H */