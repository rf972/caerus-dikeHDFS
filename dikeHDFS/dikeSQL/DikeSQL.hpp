#ifndef DIKE_SQL_HPP
#define DIKE_SQL_HPP

#include <sqlite3.h>
#include <iostream>
#include <string.h>

#include "DikeAsyncWriter.hpp"

typedef std::map<std::string, std::string> DikeSQLConfig;

class DikeSQL {    
    public:
    DikeSQL(){};

    ~DikeSQL() {
        if(workerThread.joinable()) {
            workerThread.join();
        }
    }

    int Run(DikeSQLConfig & dikeSQLConfig, DikeIO * output);

    private:
    DikeAyncWriter * dikeWriter = NULL;
    std::thread workerThread;
    sqlite3_stmt *sqlRes = NULL;
    uint64_t record_counter = 0;
    bool isRunning;

    std::thread startWorker() {
        return std::thread([=] { Worker(); });
    }

    void Worker();    
};

#endif /* DIKE_SQL_HPP */