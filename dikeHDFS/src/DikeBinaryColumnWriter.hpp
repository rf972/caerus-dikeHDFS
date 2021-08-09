#ifndef DIKE_BINARY_COLUMN_WRITER_HPP
#define DIKE_BINARY_COLUMN_WRITER_HPP

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

#include "DikeBinaryColumn.h"

class DikeBinaryColumnWriter : public DikeAsyncWriter {
    public:
    int64_t row_count = 0;
    int64_t batch_count = 0;
    int64_t data_count = 0; // Number of columns
    int64_t * data_types = NULL;
    DikeBinaryColumn_t ** columns = NULL;

    DikeBinaryColumnWriter(DikeIO * output) : DikeAsyncWriter(output) { }

    virtual ~DikeBinaryColumnWriter() override {
        if(data_types) {
            delete data_types;
        }
        if( columns ) {
            for(int i = 0; i < data_count; i++) {
                DikeBinaryColumnDestroy(columns[i]);
            }
            delete columns;
        }
    }

    virtual void close() override {
        flush();
        DikeAsyncWriter::close();
    }

    void InitializeSchema(sqlite3_stmt *sqlRes) {
        data_count = sqlite3_column_count(sqlRes);
        data_types = new int64_t [data_count];
        columns = new DikeBinaryColumn_t * [data_count];

        for(int i = 0; i < data_count; i++) {
            data_types[i] = sqlite3_column_type(sqlRes, i);
            columns[i] = new DikeBinaryColumn_t;
            int datatype = BINARY_COLUMN_TYPE_UNDEFINED;
            switch(data_types[i]) {
                case SQLITE_INTEGER:
                datatype = BINARY_COLUMN_TYPE_INT64;
                break;
                case SQLITE_FLOAT:
                datatype = BINARY_COLUMN_TYPE_DOUBLE;
                break;
                case SQLITE3_TEXT:
                datatype = BINARY_COLUMN_TYPE_BYTE_ARRAY;
                break;
            }
            DikeBinaryColumnInit(columns[i], data_types[i]);
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
        sqlite3_stmt * sqlRes = (sqlite3_stmt *)res;
        if (data_count == 0) {
            InitializeSchema(sqlRes);
        }

        if(!isRunning){
            return 0;
        }                

        int flush_needed = 0;
        dike_sqlite3_get_results(sqlRes, columns, &flush_needed);

        row_count++;
        batch_count++;
        if(flush_needed || batch_count >= BINARY_COLUMN_BATCH_SIZE){
            flush();
            batch_count = 0;
        }
        return 1;
    }

    void flush() {        
        for(int i = 0; i < data_count; i++) {
            int64_t byte_count;
            int64_t be_value;
            int rc;

            if(data_types[i] == SQLITE3_TEXT){
                byte_count = columns[i]->idx_pos - columns[i]->start_idx;
                //std::cout << i << " : byte_count : " << byte_count << std::endl;
                be_value =  htobe64(byte_count);
                rc = buffer->write(&be_value, sizeof(int64_t));
                if (rc == 0){
                    buffer = getBuffer();
                    rc = buffer->write(&be_value, sizeof(int64_t));
                }
                rc = buffer->write(columns[i]->start_idx, byte_count);
                if (rc == 0){
                    buffer = getBuffer();
                    rc = buffer->write(columns[i]->start_idx, byte_count);
                }

                columns[i]->idx_pos = columns[i]->start_idx;
            }

            byte_count = columns[i]->pos - columns[i]->start_pos;
            be_value =  htobe64(byte_count);
            rc = buffer->write(&be_value, sizeof(int64_t));
            if (rc == 0){
                buffer = getBuffer();
                rc = buffer->write(&be_value, sizeof(int64_t));
            }
            rc = buffer->write(columns[i]->start_pos, byte_count);
            if (rc == 0){
                buffer = getBuffer();
                rc = buffer->write(columns[i]->start_pos, byte_count);
            }

            columns[i]->pos = columns[i]->start_pos;
        }
    }
};

#endif /* DIKE_BINARY_COLUMN_WRITER_HPP */