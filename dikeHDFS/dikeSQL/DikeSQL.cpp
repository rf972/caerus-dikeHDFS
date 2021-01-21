#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <regex>
#include <fstream>
#include <streambuf>
#include <chrono>
#include <stdio.h>

#include <sqlite3.h>

#include "StreamReader.hpp"
#include "DikeSQL.hpp"


int DikeSQL::Run(std::istream& inStream, std::ostream& outStream, DikeSQLParam * dikeSQLParam)
{
    sqlite3 *db;    
    int rc;
    char  * errmsg;
    uint64_t record_counter = 0;
    
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0); // Disable memory statistics

    rc = sqlite3_open(":memory:", &db);
    if( rc ) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return(1);
    }

    StreamReaderParam streamReaderParam;    

    streamReaderParam.in = dynamic_cast<std::istream *>(&inStream);
    streamReaderParam.name = "S3Object";
    streamReaderParam.schema = dikeSQLParam->schema;
    rc = StreamReaderInit(db, &streamReaderParam);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't load SRD extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return 1;
    }
   
    std::string sqlCreateVirtualTable = std::string("CREATE VIRTUAL TABLE ") +  streamReaderParam.name + " USING StreamReader();";    

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

    rc = sqlite3_exec(db, sqlCreateVirtualTable.c_str(), NULL, NULL, &errmsg);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't create virtual table: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();

    sqlite3_stmt *sqlRes;
    rc = sqlite3_prepare_v2(db, dikeSQLParam->query.c_str(), -1, &sqlRes, 0);        
    if (rc != SQLITE_OK) {
        std::cerr << "Can't execute query: " << sqlite3_errmsg(db) << std::endl;        
        sqlite3_close(db);
        return 1;
    }            

    while ( (rc = sqlite3_step(sqlRes)) == SQLITE_ROW) {
        record_counter++;

        int data_count = sqlite3_data_count(sqlRes);

        for(int i = 0; i < data_count; i++) {
            const unsigned char* text = sqlite3_column_text(sqlRes, i);
            if(text){
                if(!strchr((const char*)text, ',')){
                    outStream << text << ",";
                } else {
                    outStream << "\"" << text << "\"" << ",";
                }
            }
        }
        outStream << '\n';
    }
    
    std::chrono::high_resolution_clock::time_point t3 =  std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> create_time = t2 - t1;
    std::chrono::duration<double, std::milli> select_time = t3 - t2;

    std::cout << "Records " << record_counter;
    std::cout << " create_time " << create_time.count()/ 1000 << " sec" ;
    std::cout << " select_time " << select_time.count()/ 1000 << " sec" << std::endl;

    sqlite3_finalize(sqlRes);
    sqlite3_close(db);

    return(0);
}


// cmake --build ./build/Debug
// ./build/Debug/dikeSQL/testDikeSQL ../../dike/spark/build/tpch-data/lineitem.tbl
