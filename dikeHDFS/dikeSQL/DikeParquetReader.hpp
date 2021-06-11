#ifndef DIKE_PARQUET_READER_HPP
#define DIKE_PARQUET_READER_HPP

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
#include <string.h>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerResponseImpl.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPServerSession.h>

#include <Poco/URI.h>

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/properties.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/filesystem/filesystem.h>

#include "DikeUtil.hpp"
#include "DikeBuffer.hpp"
#include "DikeIO.hpp"
#include "DikeStream.hpp"

#include "DikeSQL.hpp"
#include "DikeAsyncReader.hpp"

class DikeParquetReader: public DikeAsyncReader {
    public:  
    std::string schema = "";
    int columnCount = 0;
    std::shared_ptr<arrow::Table> table;
    std::vector<std::shared_ptr<ChunkedArray>>& columns;

    DikeParquetReader(DikeSQLConfig & dikeSQLConfig) {
        arrow::io::HdfsConnectionConfig hdfsConnectionConfig;

        std::stringstream ss;
        ss.str(dikeSQLConfig["Request"]);
        Poco::Net::HTTPRequest hdfs_req;
        hdfs_req.read(ss);

        Poco::URI uri = Poco::URI(hdfs_req.getURI());
        Poco::URI::QueryParameters uriParams = uri.getQueryParameters();        
        for(int i = 0; i < uriParams.size(); i++){
            if(uriParams[i].first.compare("namenoderpcaddress") == 0){
                std::string rpcAddress = uriParams[i].second;
                auto pos = rpcAddress.find(':');
                hdfsConnectionConfig.host = rpcAddress.substr(0, pos);
                hdfsConnectionConfig.port = std::stoi(rpcAddress.substr(pos + 1, rpcAddress.length()));
            } else if(uriParams[i].first.compare("user.name") == 0) {
                hdfsConnectionConfig.user = uriParams[i].second;                
            }
            //std::cout <<  uriParams[i].first << " = " << uriParams[i].second << std::endl;    
        }
        std::string path = uri.getPath();
        std::string fileName = path.substr(11, path.length()); // skip "/webhdfs/v1"
        std::cout <<  " Path = " << path << std::endl;
        std::cout <<  " fileName = " << fileName << std::endl;
        std::cout <<  " Host = " << hdfsConnectionConfig.host << std::endl;                
        std::cout <<  " Port = " << hdfsConnectionConfig.port << std::endl;
        std::cout <<  " User = " << hdfsConnectionConfig.user << std::endl;

        arrow::Status st;    
        
        int rowGroupIndex = std::stoi(dikeSQLConfig["Configuration.RowGroupIndex"]);

        std::shared_ptr<arrow::io::HadoopFileSystem> fs;
        st = arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);        

        std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
        st = fs->OpenReadable(fileName, &inputFile);        

        std::shared_ptr<parquet::FileMetaData> fileMetaData;    
        fileMetaData = parquet::ReadMetaData(inputFile);

        std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
        auto vt_schema = fileMetaData->schema();
        columnCount = vt_schema->num_columns();

        for(int i = 0; i < columnCount; i++) {
            auto col = (parquet::schema::PrimitiveNode*)vt_schema->GetColumnRoot(i);                        
            schema += col->name();
            if( i < columnCount - 1){
                schema += ",";
            }
        }        

        auto parquetFileReader = parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData);
        
        std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
        st = parquet::arrow::FileReader::Make(::arrow::default_memory_pool(), std::move(parquetFileReader), &arrow_reader);
        if (!st.ok()) {
            std::cout << "Handle Make (reader) error... "  << std::endl;
        }

        //arrow_reader->set_num_threads(4);

        st = arrow_reader->ReadRowGroup(rowGroupIndex, &table);
        if (!st.ok()) {
            std::cout << "Handle ReadRowGroup error... "  << std::endl;
        }

        std::cout << "Succesfully read RowGroup(1) " << table->num_rows() << " rows out of " << arrow_reader->num_row_groups() << " RowGroups" << std::endl;
        columns = table.columns();

    }

    DikeAsyncReader * getReader() {
        return (DikeAsyncReader *)this; 
    }

    ~DikeParquetReader(){
      
    }    

    int initRecord(int nCol) {
        record = new DikeRecord(nCol);
        return (record != NULL);
    }

    bool isEOF() {
        return true;
    }    
    
    virtual int getColumnCount() {
        return 16;
    }

    virtual const std::string &  getSchema() {
        return schema;
    }

    virtual int readRecord() {
        if(isEOF()){
            return 1;
        }
    }

    virtual int getColumnValue(int col, void ** value, int * len, sqlite_aff_t * affinity) {
        *affinity = SQLITE_AFF_TEXT_TERM;
        *value = record->fieldMemory[col];
        *len = 0; // record->len[col];
        return 0;
    }    
};

#endif /* DIKE_PARQUET_READER_HPP */