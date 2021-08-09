#ifndef LAMBDA_PARQUET_READER_HPP
#define LAMBDA_PARQUET_READER_HPP

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
#include <map>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerResponseImpl.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPServerSession.h>

#include <Poco/URI.h>
#include "Poco/Thread.h"

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/properties.h>
#include <parquet/column_reader.h>


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/filesystem/filesystem.h>

#include "DikeUtil.hpp"
#include "DikeProcessor.hpp"
#include "DikeBuffer.hpp"
#include "DikeIO.hpp"

class LambdaColumnReader {
    public:
    int column;
    parquet::Type::type physicalType;
    std::shared_ptr<parquet::ColumnReader> columnReader;

    parquet::Int64Reader* int64_reader = NULL;
    parquet::DoubleReader* double_reader = NULL;
    parquet::ByteArrayReader* ba_reader = NULL;

    enum {
        BATCH_SIZE = 4096, // 3.5 sec        
    };

    // This is used for caching
    int64_t * int64_values = NULL;
    double *  double_values = NULL;
    parquet::ByteArray * ba_values = NULL;
    int64_t values_size = 0;
    int64_t values_read;

    int64_t total_rows = 0;
    int64_t current_row = 0;

    LambdaColumnReader(int column, parquet::Type::type physicalType, int64_t total_rows, std::shared_ptr<parquet::ColumnReader> columnReader) {
        this->columnReader = std::move(columnReader);
        this->physicalType = physicalType;
        this->column = column;
        this->total_rows = total_rows;
 
        switch(physicalType){
            case parquet::Type::INT64:
                int64_reader = static_cast<parquet::Int64Reader*>(this->columnReader.get());
                int64_values = new int64_t [BATCH_SIZE];
            break;
            case parquet::Type::DOUBLE:
                double_reader = static_cast<parquet::DoubleReader*>(this->columnReader.get());
                double_values = new double [BATCH_SIZE];
            break;
            case parquet::Type::BYTE_ARRAY:        
               ba_reader = static_cast<parquet::ByteArrayReader*>(this->columnReader.get());
               ba_values = new parquet::ByteArray [BATCH_SIZE];
            break;
        }
    }

    ~LambdaColumnReader() {
        if (int64_values != NULL) {
            delete [] int64_values;
        }

        if (double_values != NULL) {
            delete [] double_values;
        }

        if (ba_values != NULL) {
            delete [] ba_values;
        }
    }

    int64_t readFully(int64_t * values) {
        int64_t read_size = std::min((int64_t)BATCH_SIZE, total_rows - current_row);        
        values_size = 0;
        while(values_size < read_size) {
            values_size += int64_reader->ReadBatch(read_size - values_size, 0, 0, &values[values_size], &values_read);
        }
        current_row += read_size;
        return values_size;
    }

    int64_t readFully(double * values) {
        int64_t read_size = std::min((int64_t)BATCH_SIZE, total_rows - current_row);
        values_size = 0;
        while(values_size < read_size) {
            values_size += double_reader->ReadBatch(read_size - values_size, 0, 0, &values[values_size], &values_read);
        }
        current_row += read_size;
        return values_size;
    }

    int64_t readFully(parquet::ByteArray * values) {
        int64_t read_size = std::min((int64_t)BATCH_SIZE, total_rows - current_row);
        values_size = 0;
        while(values_size < read_size) {
            values_size += ba_reader->ReadBatch(read_size - values_size, 0, 0, &values[values_size], &values_read);
        }
        current_row += read_size;
        return values_size;
    }    

    int64_t readFully() {
        int64_t rows = 0;
        //std::cout << "readFully [" << column << "] Type " << physicalType << std::endl;
        switch(physicalType){
            case parquet::Type::INT64:
                rows = readFully(int64_values);
            break;
            case parquet::Type::DOUBLE:
                rows = readFully(double_values);
            break;
            case parquet::Type::BYTE_ARRAY:        
               rows = readFully(ba_values);
            break;
        }
        return rows;
    }
};

class LambdaParquetReader{
    public:  
    std::string schema = "";
    int columnCount = 0;
    int rowCount = 0;
    int rowIdx = -1; // Current row cursor is pointing to
    int verbose = 0;
    LambdaColumnReader ** lambdaColumnReader;    
    parquet::Type::type * physicalType;
    int * columnMap; 

    std::shared_ptr<arrow::io::HadoopFileSystem> fs;
    static std::map< int, std::shared_ptr<arrow::io::HadoopFileSystem> > hadoopFileSystemMap;

    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;

    std::shared_ptr<parquet::FileMetaData> fileMetaData;
    static std::map< std::string, std::shared_ptr<parquet::FileMetaData> > fileMetaDataMap;
    static std::map< std::string, std::string > fileLastAccessTimeMap;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader;
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader;   

    LambdaParquetReader(DikeProcessorConfig & dikeProcessorConfig) {
        verbose = std::stoi(dikeProcessorConfig["system.verbose"]);
        std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

        arrow::io::HdfsConnectionConfig hdfsConnectionConfig;

        std::stringstream ss;
        ss.str(dikeProcessorConfig["Request"]);
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
        int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);

        Poco::Thread * current = Poco::Thread::current();
        int threadId = current->id();        
        
        arrow::Status st;
        if (hadoopFileSystemMap.count(threadId)) {
            fs = hadoopFileSystemMap[threadId];
            //std::cout << " LambdaParquetReader id " << threadId << " reuse FS connection "<< std::endl;
        } else {
            st = arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);
            hadoopFileSystemMap[threadId] = fs;
            //std::cout << " LambdaParquetReader id " << threadId << " create FS connection "<< std::endl;
        }
                                        
        st = fs->OpenReadable(fileName, &inputFile);        
        
        std::chrono::high_resolution_clock::time_point t2;

        if (fileMetaDataMap.count(fileName)) {
            if (0 == fileLastAccessTimeMap[fileName].compare(dikeProcessorConfig["Configuration.LastAccessTime"])){
                fileMetaData = fileMetaDataMap[fileName];
                //std::cout << " LambdaParquetReader " << fileName << " reuse fileMetaData "<< std::endl;
            } else {
                fileMetaData = std::move(parquet::ReadMetaData(inputFile));
                fileMetaDataMap[fileName] = fileMetaData;
                fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];
                //std::cout << " LambdaParquetReader " << fileName << " read fileMetaData "<< std::endl;
            }            
        } else {
            fileMetaData = std::move(parquet::ReadMetaData(inputFile));
            fileMetaDataMap[fileName] = fileMetaData;
            fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];
            //std::cout << " LambdaParquetReader " << fileName << " read fileMetaData "<< std::endl;
        }
        
        const parquet::SchemaDescriptor* schemaDescriptor = fileMetaData->schema();

        parquetFileReader = std::move(parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData));

        rowGroupReader = std::move(parquetFileReader->RowGroup(rowGroupIndex));

        rowCount = rowGroupReader->metadata()->num_rows();
        columnCount = schemaDescriptor->num_columns();
        //initRecord(columnCount);

        lambdaColumnReader = new LambdaColumnReader * [columnCount];        
        physicalType = new parquet::Type::type[columnCount];
        columnMap = new int [columnCount];
        
        for(int i = 0; i < columnCount; i++) {
            columnMap = 0; // Not in use by deafault
            auto col = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);            
            schema += col->name();
            if( i < columnCount - 1){
                schema += ",";
            }           
            
            physicalType[i] = col->physical_type();
            lambdaColumnReader[i] = NULL;
        }
        //std::cout <<  " Ready to go columnCount " << columnCount << " rowCount " << rowCount << std::endl;
        
        if(verbose) {
            t2 =  std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> create_time = t2 - t1;
            std::cout << "DikeAsyncReader constructor took " << create_time.count()/ 1000 << " sec" << std::endl;
            std::cout << "DikeAsyncReader rowCount " << rowCount << std::endl;
        }
    }

    virtual ~LambdaParquetReader() {        
        for(int i = 0; i < columnCount; i++) {
            if(lambdaColumnReader[i]) {
                delete lambdaColumnReader[i];
            }
        }
        delete [] lambdaColumnReader;         
        delete [] physicalType;
        delete []columnMap;
        parquetFileReader->Close();
        inputFile->Close();
        //fs->Disconnect();
    }

    int64_t readFully() {
        int64_t fields = 0;
        for(int i = 0; i < columnCount; i++){
            if(lambdaColumnReader[i]){
                fields += lambdaColumnReader[i]->readFully();
            }
        }
        return fields;
    }

    int initColumn(int col) {
        std::shared_ptr<parquet::ColumnReader> columnReader = rowGroupReader->Column(col);
        lambdaColumnReader[col] = new LambdaColumnReader(col, physicalType[col], rowCount, columnReader);
        //std::cout << "initColumn " << col << std::endl;
    }
};

#endif /* LAMBDA_PARQUET_READER_HPP */