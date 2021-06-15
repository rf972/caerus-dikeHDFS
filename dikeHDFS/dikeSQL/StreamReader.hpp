#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <sqlite3.h>
#include "DikeAsyncReader.hpp"

int StreamReaderInit(sqlite3 *db, DikeAsyncReader * dikeAsyncReader);

#endif /* STREAM_READER_H */