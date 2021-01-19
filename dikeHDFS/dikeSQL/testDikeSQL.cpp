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

int main (int argc, char *argv[]) 
{
    sqlite3 *db;    
    int rc;
    char  * errmsg;
    std::string sqlFileName;
    std::string sqlQuery;
    uint64_t record_counter = 0;
    
    if(argc < 2){
        std::cerr << "Usage: " << argv[0] << " file " << std::endl;        
        return 1;
    }

    sqlFileName = std::string(argv[1]);    

    sqlQuery = "SELECT * FROM S3Object";
    
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0); // Disable memory statistics

    rc = sqlite3_open(":memory:", &db);
    if( rc ) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return(1);
    }

    StreamReaderParam param;

    std::ifstream infstr(sqlFileName);

    param.in = dynamic_cast<std::istream *>(&infstr);
    param.name = "S3Object";
    param.schema = "l_orderkey INTEGER,l_partkey INTEGER,l_suppkey INTEGER,l_linenumber INTEGER,l_quantity NUMERIC,l_extendedprice NUMERIC,l_discount NUMERIC,l_tax NUMERIC,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment";
    rc = StreamReaderInit(db, &param);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't load SRD extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return 1;
    }
   
    std::string sqlCreateVirtualTable = std::string("CREATE VIRTUAL TABLE ") +  param.name + " USING StreamReader(";
    //std::string schema("l_orderkey INTEGER,l_partkey INTEGER,l_suppkey INTEGER,l_linenumber INTEGER,l_quantity NUMERIC,l_extendedprice NUMERIC,l_discount NUMERIC,l_tax NUMERIC,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment");        
    //sqlCreateVirtualTable += ", CREATE TABLE S3Object (" + schema + ")";        
    sqlCreateVirtualTable += ");";

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
    rc = sqlite3_prepare_v2(db, sqlQuery.c_str(), -1, &sqlRes, 0);        
    if (rc != SQLITE_OK) {
        std::cerr << "Can't execute query: " << sqlite3_errmsg(db) << std::endl;        
        sqlite3_close(db);
        return 1;
    }            

    while ( (rc = sqlite3_step(sqlRes)) == SQLITE_ROW) {
        record_counter++;

        if( record_counter > 5 && record_counter < 6001211){
            continue;
        }

        int data_count = sqlite3_data_count(sqlRes);

        for(int i = 0; i < data_count; i++) {
            const unsigned char* text = sqlite3_column_text(sqlRes, i);
            if(text){
                if(!strchr((const char*)text, ',')){
                    std::cout << text << ",";
                } else {
                    std::cout << "\"" << text << "\"" << ",";
                }
            }
        }
        std::cout << std::endl;
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
