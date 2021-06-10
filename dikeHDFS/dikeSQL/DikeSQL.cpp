#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <regex>
#include <fstream>
#include <streambuf>
#include <chrono>
#include <stdio.h>
#include <pthread.h>

#include <sqlite3.h>
#include "dikeSQLite3.h"

#include "StreamReader.hpp"
#include "DikeSQL.hpp"
#include "DikeUtil.hpp"

#include "DikeCsvReader.hpp"
#include "DikeParquetReader.hpp"

//int DikeSQL::Run(DikeSQLParam * dikeSQLParam, DikeIO * input, DikeIO * output)
int DikeSQL::Run(DikeSQLConfig & dikeSQLConfig, DikeIO * output)
{
    sqlite3 *db;    
    int rc;
    char * errmsg;
    
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0); // Disable memory statistics

    rc = sqlite3_open(":memory:", &db);
    if( rc ) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return(1);
    }

    DikeAsyncReader * dikeReader;
    if (dikeSQLConfig["Name"].compare("dikeSQL.parquet") == 0) {        
        dikeReader = (DikeAsyncReader *)new DikeParquetReader(dikeSQLConfig);
        std::cout << "Created parquet reader " << dikeReader << std::endl;
    } else {
        dikeReader = (DikeAsyncReader *)new DikeCsvReader(dikeSQLConfig);        
    }

    rc = StreamReaderInit(db, dikeReader);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't load SRD extention: " << std::endl;        
        return 1;
    }
   
    std::string sqlCreateVirtualTable = "CREATE VIRTUAL TABLE S3Object USING StreamReader();";

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

    rc = sqlite3_exec(db, sqlCreateVirtualTable.c_str(), NULL, NULL, &errmsg);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't create virtual table: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
    
    rc = sqlite3_prepare_v2(db, dikeSQLConfig["Configuration.Query"].c_str(), -1, &sqlRes, 0);        
    if (rc != SQLITE_OK) {
        std::cerr << "Can't execute query: " << sqlite3_errmsg(db) << std::endl;        
        sqlite3_close(db);
        return 1;
    }                
     
    dikeWriter = new DikeAyncWriter(output);
    
    isRunning = true;
    workerThread = startWorker();
    dikeWriter->Worker();
    isRunning = false;
    if(workerThread.joinable()){
        workerThread.join();
    }

#if 0
    std::chrono::high_resolution_clock::time_point t3 =  std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> create_time = t2 - t1;
    std::chrono::duration<double, std::milli> select_time = t3 - t2;
    std::cout << "Records " << record_counter;
    std::cout << " create_time " << create_time.count()/ 1000 << " sec" ;
    std::cout << " select_time " << select_time.count()/ 1000 << " sec" << std::endl;
#endif

    delete dikeWriter;
    delete dikeReader;

    //std::cout << "Message : " << sqlite3_errmsg(db) << std::endl;

    sqlite3_finalize(sqlRes);
    sqlite3_close(db);

    return(0);
}

void DikeSQL::Worker()
{
    int sqlite3_rc;
    int writer_rc;
    const char * res[128];
    int data_count;
    int total_bytes;

    //std::thread::id thread_id = std::this_thread::get_id();
    pthread_t thread_id = pthread_self();
    pthread_setname_np(thread_id, "DikeSQL::Worker");

    //outStream.exceptions ( std::ostream::failbit | std::ostream::badbit );
    try {
        sqlite3_rc = sqlite3_step(sqlRes);
        writer_rc = 1;
        while (isRunning && SQLITE_ROW == sqlite3_rc && writer_rc) {
            record_counter++;            
            data_count = dike_sqlite3_get_data(sqlRes, res, 128, &total_bytes);
            if(total_bytes > 0){
                writer_rc = dikeWriter->write(res, data_count, ',', '\n', total_bytes);
            }
            sqlite3_rc = sqlite3_step(sqlRes);
        }
        //dikeWriter->write('\n');        
    } catch (...) {
        std::cout << "Caught exception " << std::endl;
    }

    dikeWriter->close();

    //std::cout << "DikeSQL::Worker exiting " << std::endl;
    //std::cout << "DikeSQL::Worker sqlite3_rc " << sqlite3_rc << std::endl;
    //std::cout << "DikeSQL::Worker writer_rc " << writer_rc << std::endl;
    //std::cout << "DikeSQL::Worker isRunning " << isRunning << std::endl;
}

// cmake --build ./build/Debug
// ./build/Debug/dikeSQL/testDikeSQL ../../dike/spark/build/tpch-data/lineitem.tbl
