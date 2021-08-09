#ifndef DIKE_BINARY_WRITER_HPP
#define DIKE_BINARY_WRITER_HPP

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

class DikeBinaryWriter : public DikeAsyncWriter {
    public:
    int64_t row_count = 0;
    int64_t data_count = 0; // Number of columns
    int64_t * data_types = NULL;

    DikeBinaryWriter(DikeIO * output) : DikeAsyncWriter(output) { }

    virtual ~DikeBinaryWriter() override {
        if(data_types) {
            delete data_types;
        }
    }

    virtual void close() override {
        DikeAsyncWriter::close();
    }

    void InitializeSchema(sqlite3_stmt *sqlRes) {
        data_count = sqlite3_column_count(sqlRes);
        data_types = new int64_t [data_count];
        for(int i = 0; i < data_count; i++) {
            data_types[i] = sqlite3_column_type(sqlRes, i);
        }
        // This is our first write, so buffer should have enough space
        int64_t be_value = htobe64(data_count);
        buffer->write(&be_value, sizeof(int64_t));
        for(int i = 0; i < data_count; i++) {
            be_value = htobe64(data_types[i]);
            //std::cout << i << " : " << data_types[i] << std::endl;
            buffer->write(&be_value, sizeof(int64_t));
        }
    }

    virtual int write(void * res) override {
        sqlite3_stmt *sqlRes = (sqlite3_stmt *)res;
        int rc = 0;

        if (data_count == 0) {
            InitializeSchema(sqlRes);
        }

        if(!isRunning){
            return 0;
        }        
  
        int64_t be_value;
        for(int i = 0; i < data_count; i++) {
            //int data_type = sqlite3_column_type(sqlRes, i);
            switch(data_types[i]) {
                case SQLITE_INTEGER:
                {
                    int64_t int64_value = sqlite3_column_int64(sqlRes, i);
                    be_value = htobe64(int64_value);
                    rc = buffer->write(&be_value, sizeof(int64_t));
                    if (rc == 0){
                        buffer = getBuffer();
                        rc = buffer->write(&be_value, sizeof(int64_t));
                    }
                }
                break;
                case SQLITE_FLOAT:
                {
                    double double_value = sqlite3_column_double(sqlRes, i);
                    be_value = htobe64(*(int64_t*)&double_value);
                    rc = buffer->write(&be_value, sizeof(double));
                    if (rc == 0){
                        buffer = getBuffer();
                        rc = buffer->write(&be_value, sizeof(double));
                    }
                }
                break;
                case SQLITE3_TEXT:
                {
                    uint32_t column_bytes = sqlite3_column_bytes(sqlRes, i);
                    const uint8_t* column_text = sqlite3_column_text(sqlRes, i);
                    rc = buffer->writeUTF8(column_text, column_bytes);
                    if (rc == 0){
                        buffer = getBuffer();
                        rc = buffer->writeUTF8(column_text, column_bytes);
                    }
                }
                break;                        
            }
        }

        row_count++;
        return rc;
    }
};

#endif /* DIKE_BINARY_WRITER_HPP */