#ifndef DIKE_CSV_WRITER_HPP
#define DIKE_CSV_WRITER_HPP

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

#include "DikeUtil.hpp"
#include "DikeIO.hpp"
#include "DikeBuffer.hpp"
#include "DikeAsyncWriter.hpp"

class DikeCsvWriter : public DikeAsyncWriter {
    public:

    DikeCsvWriter(DikeIO * output) : DikeAsyncWriter(output) { }

    virtual int write(void * res) override {
        sqlite3_stmt * sqlRes = (sqlite3_stmt *) res;
        const char * buffer[128];
        int total_bytes;
        int data_count = dike_sqlite3_get_data(sqlRes, buffer, 128, &total_bytes);       
        int rc = write(buffer, data_count, ',', '\n', total_bytes);
        
        return rc;
    }

    int write(const char **res, int data_count, char delim, char term, int total_bytes) {
        if(!isRunning){
            return 0;
        }        

        // We need to terminate each field, so we do (+ data_count)
        int rc = buffer->write(res, data_count, delim, term, total_bytes + data_count);        
        if(rc) {           
            return rc;
        }
        buffer = getBuffer();
        rc = buffer->write(res, data_count, delim, term, total_bytes + data_count);

        if(!rc) {           
            std::cout << "DikeCsvWriter::write failed" << std::endl;
            std::cout << "total_bytes " << total_bytes << std::endl;
            std::cout << "buffer size " << buffer->getSize() << std::endl;
        }

        return rc;
    }

    int write(char term) {
        if(!isRunning) {
            return 0;
        }        

        int rc = buffer->write(term);       
        if(rc) {
            return rc;
        }
        buffer = getBuffer();
        rc = buffer->write(term);        
        return rc;
    }
};

#endif /* DIKE_CSV_WRITER_HPP */