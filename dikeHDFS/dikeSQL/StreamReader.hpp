#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <sqlite3.h>
#include <iostream>
#include <string.h>


struct StreamReaderParam {
    std::istream * in;
    std::string name;
    std::string schema;    
};

int StreamReaderInit(sqlite3 *db, StreamReaderParam * param);

#endif /* STREAM_READER_H */