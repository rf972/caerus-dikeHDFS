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

class DikeColumnReader {
    public:
    int column;
    parquet::Type::type physicalType;
    std::shared_ptr<parquet::ColumnReader> columnReader;

    parquet::Int64Reader* int64_reader = NULL;
    parquet::DoubleReader* double_reader = NULL;
    parquet::ByteArrayReader* ba_reader = NULL;

    DikeColumnReader(int column, parquet::Type::type physicalType, std::shared_ptr<parquet::ColumnReader> columnReader) {
        this->columnReader = std::move(columnReader);
        this->physicalType = physicalType;
        this->column = column;
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

    }

    int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, int64_t * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->ReadBatch type "  << parquet::TypeToString(physicalType) << std::endl;        
        //int64_reader->HasNext();
        int64_t rows_read = int64_reader->ReadBatch(batch_size, def_levels, rep_levels, values, values_read);        
        return rows_read;
    }

    int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, double * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->ReadBatch type "  << parquet::TypeToString(physicalType) << std::endl;
        return double_reader->ReadBatch(batch_size, def_levels, rep_levels, values, values_read);
    }

    int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, parquet::ByteArray * values, int64_t* values_read) {
        //std::cout <<  "DikeColumnReader[" << column << "]->ReadBatch type "  << parquet::TypeToString(physicalType) << std::endl;
        return ba_reader->ReadBatch(batch_size, def_levels, rep_levels, values, values_read);
    }

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
    int rowIdx = 0; // Current row cursor is pointing to
    DikeColumnReader ** dikeColumnReader;    
    parquet::Type::type * physicalType;
    int * accessMap;

    std::shared_ptr<arrow::io::HadoopFileSystem> fs;
    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;

    std::shared_ptr<parquet::FileMetaData> fileMetaData;
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader;
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader;   

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
#if 0        
        std::cout <<  " Path = " << path << std::endl;
        std::cout <<  " fileName = " << fileName << std::endl;
        std::cout <<  " Host = " << hdfsConnectionConfig.host << std::endl;                
        std::cout <<  " Port = " << hdfsConnectionConfig.port << std::endl;
        std::cout <<  " User = " << hdfsConnectionConfig.user << std::endl;
#endif
        arrow::Status st;    
        
        int rowGroupIndex = std::stoi(dikeSQLConfig["Configuration.RowGroupIndex"]);
        
        st = arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);
        st = fs->OpenReadable(fileName, &inputFile);        
            
        fileMetaData = std::move(parquet::ReadMetaData(inputFile));

        //std::unique_ptr<RowGroupMetaData> rowGroupMetaData = fileMetaData.RowGroup(rowGroupIndex);

        //std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
        const parquet::SchemaDescriptor* schemaDescriptor = fileMetaData->schema();

        parquetFileReader = std::move(parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData));
        rowGroupReader = std::move(parquetFileReader->RowGroup(rowGroupIndex));

        rowCount = rowGroupReader->metadata()->num_rows();
        columnCount = schemaDescriptor->num_columns();
        initRecord(columnCount);

        dikeColumnReader = new DikeColumnReader * [columnCount];        
        physicalType = new parquet::Type::type[columnCount];
        accessMap = new int [columnCount];
        
        for(int i = 0; i < columnCount; i++) {
            auto col = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);            
            schema += col->name();
            if( i < columnCount - 1){
                schema += ",";
            }           
            
            physicalType[i] = col->physical_type();            
            std::shared_ptr<parquet::ColumnReader> columnReader = rowGroupReader->Column(i);
            dikeColumnReader[i] = new DikeColumnReader(i, physicalType[i], columnReader);
            accessMap[i] = 0;
        }
        //std::cout <<  " Ready to go columnCount " << columnCount << " rowCount " << rowCount << std::endl;
    }

    DikeAsyncReader * getReader() {
        return (DikeAsyncReader *)this; 
    }

    ~DikeParquetReader() {        
        delete record;
        for(int i = 0; i < columnCount; i++) {
            delete dikeColumnReader[i];                   
        }
        delete dikeColumnReader;         
        delete physicalType;
        delete accessMap;
    }    

    int initRecord(int nCol) {
        record = new DikeRecord(nCol);
        return (record != NULL);
    }

    bool isEOF() {
        //return true;
        if(rowIdx < rowCount){
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
        //std::cout <<  "readRecord " << rowIdx << std::endl;
        if(rowIdx > 0){
            for (int i  = 0; i < columnCount; i++){
                if(accessMap[i] == 0) {
                    dikeColumnReader[i]->Skip(1);
                }
                accessMap[i] = 0;
            }
        }
        rowIdx++;
        return 0;
    }

    virtual int getColumnValue(int col, void ** value_ptr, int * len, sqlite_aff_t * affinity) {
        int64_t values_read = 0;
        int64_t rows_read = 0;
 
        accessMap[col] = 1;

        //std::cout <<  "getColumnValue " << col << std::endl;

        switch(physicalType[col]){
            case parquet::Type::INT64:
            {
                //parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(columnReader[col].get());
                int64_t value;
                rows_read = dikeColumnReader[col]->ReadBatch(1, NULL, NULL, &value, &values_read);

                *affinity = SQLITE_AFF_INTEGER;
                *len = sizeof(int64_t);
                memcpy ( record->fieldMemory[col], &value, sizeof(int64_t));
                //std::cout <<  "rows_read " << rows_read << " values_read " << values_read << std::endl;
            }
            break;
            case parquet::Type::DOUBLE:
            {
                //parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(columnReader[col].get());
                double value;
                rows_read = dikeColumnReader[col]->ReadBatch(1, nullptr, nullptr, &value, &values_read);

                *affinity = SQLITE_AFF_NUMERIC;
                *len = sizeof(double);
                memcpy ( record->fieldMemory[col], &value, *len);                
            }     
            break;
            case parquet::Type::BYTE_ARRAY:
            {
                //parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(columnReader[col].get());
                parquet::ByteArray value;
                rows_read = dikeColumnReader[col]->ReadBatch(1, NULL, NULL, &value, &values_read);
                // TODO fix me (it does not belong here)
                for(int i = 0; i < value.len; i++) {
                    if(value.ptr[i] == ','){
                        *affinity = SQLITE_AFF_TEXT_TERM;
                        record->fieldMemory[col][0] = '\"';
                        memcpy ( &record->fieldMemory[col][1], value.ptr, value.len);
                        record->fieldMemory[col][value.len + 1] = '\"';
                        record->fieldMemory[col][value.len + 2] = 0;
                        *len = value.len + 3;
                        *value_ptr = record->fieldMemory[col];
                        return 0;                          
                    }
                }

                *affinity = SQLITE_AFF_TEXT_TERM;
                memcpy ( record->fieldMemory[col], value.ptr, value.len);
                record->fieldMemory[col][value.len] = 0;
                *len = value.len + 1;
            }
            break;
        }
        *value_ptr = record->fieldMemory[col];
        return 0;
    }    
};

#endif /* DIKE_PARQUET_READER_HPP */