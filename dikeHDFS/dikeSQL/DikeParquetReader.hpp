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
#include "DikeBuffer.hpp"
#include "DikeIO.hpp"
#include "DikeStream.hpp"

#include "DikeSQL.hpp"
#include "DikeAsyncReader.hpp"

//#define SAFE_MODE 1

class DikeColumnReader {
    public:
    int column;
    int64_t current_row;
    int64_t buffer_row;
    parquet::Type::type physicalType;
    std::shared_ptr<parquet::ColumnReader> columnReader;
    double total_read_time = 0;

    parquet::Int64Reader* int64_reader = NULL;
    parquet::DoubleReader* double_reader = NULL;
    parquet::ByteArrayReader* ba_reader = NULL;

    enum {
        BUFFER_SIZE = 2048,
        //BUFFER_SIZE = 10,
    };

    parquet::ByteArray ba_value;

    // This is used for caching
    int64_t * int64_values = NULL;
    double *  double_values = NULL;
    parquet::ByteArray * ba_values = NULL;
    int64_t values_size = 0;

    DikeColumnReader(int column, parquet::Type::type physicalType, std::shared_ptr<parquet::ColumnReader> columnReader) {
        this->columnReader = std::move(columnReader);
        this->physicalType = physicalType;
        this->column = column;
        this->current_row = 0;
        this->buffer_row = 0;
        //std::cout <<  "DikeColumnReader[" << column << "] type "  << parquet::TypeToString(physicalType) << std::endl;

        switch(physicalType){
            case parquet::Type::INT64:
                int64_reader = static_cast<parquet::Int64Reader*>(this->columnReader.get());
            break;
            case parquet::Type::DOUBLE:
                double_reader = static_cast<parquet::DoubleReader*>(this->columnReader.get());
            break;
            case parquet::Type::BYTE_ARRAY:        
               ba_reader = static_cast<parquet::ByteArrayReader*>(this->columnReader.get());
            break;
        }
    }

    ~DikeColumnReader() {
        // std::cout <<  "~DikeColumnReader[" << column << "]" << std::endl;
        // Should we take care of columnReader here ?
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


    int64_t GetValue(int64_t row, int64_t * value) {
        
        if (int64_values == NULL) {
            int64_values = new int64_t [BUFFER_SIZE];
        }
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0) {
            current_row += skip_rows;
            if(buffer_row > 0) {  // We have a buffer
                buffer_row += skip_rows;
                if(buffer_row >= values_size) { // Check if skip within the buffer
                    skip_rows = buffer_row - values_size;
                    buffer_row = 0;
                    int64_reader->Skip(skip_rows);
                }
            } else { // We do not have a buffer, just skip
                int64_reader->Skip(skip_rows);
            }
        }

        // We done with skip logic
        if(buffer_row == 0) {  // We do not have a buffer, read it
            int64_t values_read;
            
            std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();
            // We may need to handle out of bound read
            int64_t rows_read = int64_reader->ReadBatch(BUFFER_SIZE, 0, 0, int64_values, &values_read);
            values_size = rows_read;

            std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> read_time = t2 - t1;
            total_read_time += read_time.count();

        }

        // We done with buffer
        *value = int64_values[buffer_row];
        buffer_row++;
        current_row ++;
        if(buffer_row >= values_size){
            buffer_row = 0;
        }

        return 1;
    }

    int64_t Read(int64_t row, int16_t* def_levels, int16_t* rep_levels, int64_t * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->Read row "  << row << std::endl;                
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0){
            int64_reader->Skip(skip_rows);
            current_row += skip_rows;
        }
        int64_t rows_read = int64_reader->ReadBatch(1, def_levels, rep_levels, values, values_read);
        current_row += rows_read;
        return rows_read;
    }

    int64_t GetValue(int64_t row, double * value) {
        
        if (double_values == NULL) {
            double_values = new double [BUFFER_SIZE];
        }
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0) {
            current_row += skip_rows;
            if(buffer_row > 0) {  // We have a buffer
                buffer_row += skip_rows;
                if(buffer_row >= values_size) { // Check if skip within the buffer
                    skip_rows = buffer_row - values_size;
                    buffer_row = 0;
                    double_reader->Skip(skip_rows);
                }
            } else { // We do not have a buffer, just skip
                double_reader->Skip(skip_rows);
            }
        }

        // We done with skip logic
        if(buffer_row == 0) {  // We do not have a buffer, read it
            int64_t values_read;
            std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();
            // We may need to handle out of buond read
            int64_t rows_read = double_reader->ReadBatch(BUFFER_SIZE, 0, 0, double_values, &values_read);
            values_size = rows_read;

            std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> read_time = t2 - t1;
            total_read_time += read_time.count();

        }

        // We done with buffer
        *value = double_values[buffer_row];
        buffer_row++;
        current_row ++;
        if(buffer_row >= values_size){
            buffer_row = 0;
        }

        return 1;
    }

//#ifdef SAFE_MODE
    int64_t Read(int64_t row, int16_t* def_levels, int16_t* rep_levels, double * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->Read row "  << row << std::endl;        
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0){
            double_reader->Skip(skip_rows);
            current_row += skip_rows;
        }
        int64_t rows_read = double_reader->ReadBatch(1, def_levels, rep_levels, values, values_read);
        current_row += rows_read;
        return rows_read;        
    }
//#endif


    int64_t GetValue(int64_t row, parquet::ByteArray * value) {
        
        if (ba_values == NULL) {
            ba_values = new parquet::ByteArray [BUFFER_SIZE];
        }
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0) {
            current_row += skip_rows;
            if(buffer_row > 0) {  // We have a buffer
                buffer_row += skip_rows;
                if(buffer_row >= values_size) { // Check if skip within the buffer
                    skip_rows = buffer_row - values_size;
                    buffer_row = 0;
                    ba_reader->Skip(skip_rows);
                }
            } else { // We do not have a buffer, just skip
                ba_reader->Skip(skip_rows);
            }
        }

        // We done with skip logic
        if(buffer_row == 0) {  // We do not have a buffer, read it
            int64_t values_read;
            std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

            // We may need to handle out of buond read
            int64_t rows_read = ba_reader->ReadBatch(BUFFER_SIZE, 0, 0, ba_values, &values_read);
            values_size = rows_read;

            std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> read_time = t2 - t1;
            total_read_time += read_time.count();

        }

        // We done with buffer
        *value = ba_values[buffer_row];
        buffer_row++;
        current_row ++;
        if(buffer_row >= values_size){
            buffer_row = 0;
        }

        return 1;
    }

#ifdef SAFE_MODE
    int64_t Read(int64_t row, int16_t* def_levels, int16_t* rep_levels, parquet::ByteArray * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->Read row "  << row << std::endl;        
        int64_t skip_rows = row - current_row;
        if(skip_rows > 0){
            ba_reader->Skip(skip_rows);
            current_row += skip_rows;
        }
        //int64_t rows_read = ba_reader->ReadBatch(1, def_levels, rep_levels, values, values_read);
        int64_t rows_read = ba_reader->ReadBatch(1, def_levels, rep_levels, &ba_value, values_read);
        *values = ba_value;
        current_row += rows_read;
        return rows_read;        
         
    }
#endif

    int64_t Skip(int64_t num_rows_to_skip) {
        //std::cout <<  "DikeColumnReader[" << column << "]->Skip type "  << parquet::TypeToString(physicalType) << std::endl;
        switch(physicalType){
            case parquet::Type::INT64:
                return int64_reader->Skip(num_rows_to_skip);
            break;
            case parquet::Type::DOUBLE:
                return double_reader->Skip(num_rows_to_skip);
            break;
            case parquet::Type::BYTE_ARRAY:        
               return ba_reader->Skip(num_rows_to_skip);
            break;
        }
    }
};

class DikeParquetReader: public DikeAsyncReader {
    public:  
    std::string schema = "";
    int columnCount = 0;
    int rowCount = 0;
    int rowIdx = -1; // Current row cursor is pointing to
    int verbose = 0;
    DikeColumnReader ** dikeColumnReader;    
    parquet::Type::type * physicalType;

    std::shared_ptr<arrow::io::HadoopFileSystem> fs;
    static std::map< int, std::shared_ptr<arrow::io::HadoopFileSystem> > hadoopFileSystemMap;

    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;

    std::shared_ptr<parquet::FileMetaData> fileMetaData;
    static std::map< std::string, std::shared_ptr<parquet::FileMetaData> > fileMetaDataMap;
    static std::map< std::string, std::string > fileLastAccessTimeMap;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader;
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader;   

    DikeParquetReader(DikeSQLConfig & dikeSQLConfig) {
        verbose = std::stoi(dikeSQLConfig["system.verbose"]);
        std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

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
        int rowGroupIndex = std::stoi(dikeSQLConfig["Configuration.RowGroupIndex"]);

        Poco::Thread * current = Poco::Thread::current();
        int threadId = current->id();        
        
        arrow::Status st;
        if (hadoopFileSystemMap.count(threadId)) {
            fs = hadoopFileSystemMap[threadId];
            //std::cout << " DikeParquetReader id " << threadId << " reuse FS connection "<< std::endl;
        } else {
            st = arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);
            hadoopFileSystemMap[threadId] = fs;
            //std::cout << " DikeParquetReader id " << threadId << " create FS connection "<< std::endl;
        }
                                        
        st = fs->OpenReadable(fileName, &inputFile);        
        
        if (fileMetaDataMap.count(fileName)) {
            if (0 == fileLastAccessTimeMap[fileName].compare(dikeSQLConfig["Configuration.LastAccessTime"])){
                fileMetaData = fileMetaDataMap[fileName];
                //std::cout << " DikeParquetReader " << fileName << " reuse fileMetaData "<< std::endl;
            } else {
                fileMetaData = std::move(parquet::ReadMetaData(inputFile));
                fileMetaDataMap[fileName] = fileMetaData;
                fileLastAccessTimeMap[fileName] = dikeSQLConfig["Configuration.LastAccessTime"];
                //std::cout << " DikeParquetReader " << fileName << " read fileMetaData "<< std::endl;
            }            
        } else {
            fileMetaData = std::move(parquet::ReadMetaData(inputFile));
            fileMetaDataMap[fileName] = fileMetaData;
            fileLastAccessTimeMap[fileName] = dikeSQLConfig["Configuration.LastAccessTime"];
            //std::cout << " DikeParquetReader " << fileName << " read fileMetaData "<< std::endl;
        }
        
        const parquet::SchemaDescriptor* schemaDescriptor = fileMetaData->schema();

        parquetFileReader = std::move(parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData));
        rowGroupReader = std::move(parquetFileReader->RowGroup(rowGroupIndex));

        rowCount = rowGroupReader->metadata()->num_rows();
        columnCount = schemaDescriptor->num_columns();
        initRecord(columnCount);

        dikeColumnReader = new DikeColumnReader * [columnCount];        
        physicalType = new parquet::Type::type[columnCount];
        
        for(int i = 0; i < columnCount; i++) {
            auto col = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);            
            schema += col->name();
            if( i < columnCount - 1){
                schema += ",";
            }           
            
            physicalType[i] = col->physical_type();            
            std::shared_ptr<parquet::ColumnReader> columnReader = rowGroupReader->Column(i);
            dikeColumnReader[i] = new DikeColumnReader(i, physicalType[i], columnReader);
        }
        //std::cout <<  " Ready to go columnCount " << columnCount << " rowCount " << rowCount << std::endl;
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        if(verbose) {
            std::chrono::duration<double, std::milli> create_time = t2 - t1;
            std::cout << "DikeAsyncReader constructor took " << create_time.count()/ 1000 << " sec" << std::endl;
            std::cout << "DikeAsyncReader rowCount " << rowCount << std::endl;
        }
    }

    DikeAsyncReader * getReader() {
        return (DikeAsyncReader *)this; 
    }

    virtual ~DikeParquetReader() {        
        Poco::Thread * current = Poco::Thread::current();
        //std::cout << "~DikeParquetReader id " << current->id() << std::endl;

        if(verbose) {
            double total_read_time = 0;
            for(int i = 0; i < columnCount; i++) {
                total_read_time += dikeColumnReader[i]->total_read_time;
            }
            std::cout << "DikeAsyncReader total_read_time " << std::fixed << total_read_time / 1000 << " sec" << std::endl;
        }

        delete record;
        for(int i = 0; i < columnCount; i++) {
            delete dikeColumnReader[i];                   
        }
        delete [] dikeColumnReader;         
        delete [] physicalType;
        inputFile->Close();
        //fs->Disconnect();
    }    

    int initRecord(int nCol) {
        record = new DikeRecord(nCol);
        return (record != NULL);
    }

    bool isEOF() {
        //return true;
        if(rowIdx < rowCount - 1){
            return false;
        }
        return true;
    }    
    
    virtual int getColumnCount() {
        return columnCount;
    }

    virtual const std::string &  getSchema() {
        return schema;
    }

    virtual int readRecord() {
        if(isEOF()){
            return 1;
        }        
        
        rowIdx++;
        //std::cout <<  "readRecord " << rowIdx << std::endl;
        for (int col  = 0; col < columnCount; col++){
            record->len[col] = 0;
            record->fields[col] = 0;
        }
        return 0;
    }

    sqlite_aff_t getAffinity(int col) {
        sqlite_aff_t affinity = SQLITE_AFF_INVALID;
        switch(physicalType[col]){
            case parquet::Type::INT64:
                affinity = SQLITE_AFF_INTEGER;
                break;
            case parquet::Type::DOUBLE:
                affinity = SQLITE_AFF_NUMERIC;
                break;
            case parquet::Type::BYTE_ARRAY:
                //affinity = SQLITE_AFF_TEXT_TERM;
                affinity = SQLITE_AFF_TEXT;
                break;
        }
        return  affinity;               
    }

    virtual int getColumnValue(int col, void ** value_ptr, int * len, sqlite_aff_t * affinity) {
        int64_t values_read = 0;
        int64_t rows_read = 0;
 
        *affinity = getAffinity(col);
        if(record->len[col] > 0){
            *value_ptr = record->fields[col];
            *len = record->len[col];            
            return 0;
        }

        //accessMap[col] = 1;
        //std::cout <<  "getColumnValue " << col << std::endl;

        switch(physicalType[col]){
            case parquet::Type::INT64:
            {
#ifdef SAFE_MODE                
                //parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(columnReader[col].get());
                int64_t value;
                rows_read = dikeColumnReader[col]->Read(rowIdx, NULL, NULL, &value, &values_read);

                // *affinity = SQLITE_AFF_INTEGER;                
                memcpy ( record->fieldMemory[col], &value, sizeof(int64_t));
                //std::cout <<  "rows_read " << rows_read << " values_read " << values_read << std::endl;
                record->fields[col] = record->fieldMemory[col];                
#else
                dikeColumnReader[col]->GetValue(rowIdx, (int64_t *) record->fieldMemory[col]);
                record->fields[col] = record->fieldMemory[col];        
#endif
                *len = sizeof(int64_t);
            }
            
            break;
            case parquet::Type::DOUBLE:
            {
                //parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(columnReader[col].get());
#ifdef SAFE_MODE
                double value;
                rows_read = dikeColumnReader[col]->Read(rowIdx, nullptr, nullptr, &value, &values_read);
                // *affinity = SQLITE_AFF_NUMERIC;
                *len = sizeof(double);
                memcpy ( record->fieldMemory[col], &value, *len);
                record->fields[col] = record->fieldMemory[col];
#else                
                dikeColumnReader[col]->GetValue(rowIdx, (double *) record->fieldMemory[col]);
                record->fields[col] = record->fieldMemory[col];                
#endif                
                *len = sizeof(double);
            }     
            break;
            case parquet::Type::BYTE_ARRAY:
            {
                //parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(columnReader[col].get());                
                parquet::ByteArray value;
#ifdef SAFE_MODE                
                rows_read = dikeColumnReader[col]->Read(rowIdx, NULL, NULL, &value, &values_read);
                memcpy ( record->fieldMemory[col], value.ptr, value.len);
                record->fieldMemory[col][value.len] = 0;
                record->fields[col] = record->fieldMemory[col];
                *len = value.len;

#else                
                dikeColumnReader[col]->GetValue(rowIdx, &value);
                record->fields[col] = (uint8_t *) value.ptr;
                *len = value.len;
#endif
            }
            break;
        }
        *value_ptr = record->fields[col];
        record->len[col] = *len; // this is for caching 
        return 0;
    }    
};

std::map<int, std::shared_ptr<arrow::io::HadoopFileSystem> > DikeParquetReader::hadoopFileSystemMap;
std::map< std::string, std::shared_ptr<parquet::FileMetaData> >  DikeParquetReader::fileMetaDataMap;
std::map< std::string, std::string > DikeParquetReader::fileLastAccessTimeMap;

#endif /* DIKE_PARQUET_READER_HPP */