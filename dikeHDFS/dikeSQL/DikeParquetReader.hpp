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
    
    DikeParquetReader(DikeSQLConfig & dikeSQLConfig) {        
        arrow::Status st;    
        std::shared_ptr<arrow::Table> table;

        // https://gist.github.com/hurdad/06058a22ca2b56e25d63aaa6f3a9108f
        arrow::io::HdfsConnectionConfig conf;
        conf.host = "dikehdfs";	        
        conf.port = 9000;
        conf.user = "peter";

        std::shared_ptr<arrow::io::HadoopFileSystem> fs;
        st = arrow::io::HadoopFileSystem::Connect(&conf, &fs);        

        std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
        st = fs->OpenReadable("/lineitem.parquet", &inputFile);        

        std::shared_ptr<parquet::FileMetaData> fileMetaData;    
        fileMetaData = parquet::ReadMetaData(inputFile);

        std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
        auto schema = fileMetaData->schema();

        for(auto i = 0; i < schema->num_columns(); i++) {
            auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(i);
            
            std::cout << col->name() << ",";
        }
        std::cout << std::endl;

        auto parquetFileReader = parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData);
        
        std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
        st = parquet::arrow::FileReader::Make(::arrow::default_memory_pool(), std::move(parquetFileReader), &arrow_reader);
        if (!st.ok()) {
            std::cout << "Handle Make (reader) error... "  << std::endl;
        }

        st = arrow_reader->ReadRowGroup(1, &table);
        if (!st.ok()) {
            std::cout << "Handle ReadRowGroup error... "  << std::endl;
        }

        std::cout << "Succesfully read RowGroup(1) " << table->num_rows() << " rows out of " << arrow_reader->num_row_groups() << " RowGroups" << std::endl;
    }

    DikeAsyncReader * getReader() {
        return (DikeAsyncReader *)this; 
    }

    ~DikeParquetReader(){
      
    }    

    virtual int initRecord(int nCol) {
        record = new DikeRecord(nCol);
        return (record != NULL);
    }

    bool isEOF() {
        return true;
    }    
    
    virtual int getColumnCount() {
        return 16;
    }

    virtual int readRecord() {
        if(isEOF()){
            return 1;
        }
    }
};

#endif /* DIKE_PARQUET_READER_HPP */