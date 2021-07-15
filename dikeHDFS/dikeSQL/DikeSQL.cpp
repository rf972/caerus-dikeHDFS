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

#include "DikeCsvWriter.hpp"
#include "DikeParquetWriter.hpp"

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
        dikeWriter = new DikeParquetWriter(output);
        //std::cout << "Created parquet reader " << dikeReader << std::endl;
    } else {
        dikeReader = (DikeAsyncReader *)new DikeCsvReader(dikeSQLConfig);
        dikeWriter = new DikeCsvWriter(output);
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
    
    isRunning = true;
    workerThread = startWorker();
    dikeWriter->Worker();
    isRunning = false;
    if(workerThread.joinable()){
        workerThread.join();
    }

    if (std::stoi(dikeSQLConfig["system.verbose"]) > 0) {
        std::chrono::high_resolution_clock::time_point t3 =  std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> create_time = t2 - t1;
        std::chrono::duration<double, std::milli> select_time = t3 - t2;
        std::cout << "Records " << record_counter;
        std::cout << " create_time " << create_time.count()/ 1000 << " sec" ;
        std::cout << " select_time " << select_time.count()/ 1000 << " sec" << std::endl;
    }

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
    int writer_rc = 1;    
    pthread_t thread_id = pthread_self();
    pthread_setname_np(thread_id, "DikeSQL::Worker");
    
    try {    
        while (isRunning && SQLITE_ROW == sqlite3_step(sqlRes) && writer_rc) {
            record_counter++;
            writer_rc = dikeWriter->write(sqlRes);            
        }
    } catch (...) {
        std::cout << "Caught exception " << std::endl;
    }

    dikeWriter->close();
}