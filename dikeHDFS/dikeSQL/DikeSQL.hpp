#ifndef DIKE_SQL_HPP
#define DIKE_SQL_HPP

#include <sqlite3.h>
#include <iostream>
#include <string.h>

#include "Poco/Net/StreamSocket.h"

#include "DikeAsyncWriter.hpp"

struct DikeSQLParam {    
    std::string query;
    std::string schema;
    uint64_t    blockOffset;
    uint64_t    blockSize;
};

class DikeSQL {    
    public:
    DikeSQL(){};

    ~DikeSQL() {
        if(workerThread.joinable()){
            workerThread.join();
        }
    }

    DikeAyncWriter * dikeWriter = NULL;
    std::thread workerThread;
    sqlite3_stmt *sqlRes = NULL;
    uint64_t record_counter = 0;
    bool isRunning;

    std::thread startWorker() {
        return std::thread([=] { Worker(); });
    }

    void Worker();

    int Run(DikeSQLParam * dikeSQLParam,
        Poco::Net::HTTPSession  * inSession,
        std::istream            * inStream,
        Poco::Net::StreamSocket * outSocket
    );
};

#endif /* DIKE_SQL_HPP */