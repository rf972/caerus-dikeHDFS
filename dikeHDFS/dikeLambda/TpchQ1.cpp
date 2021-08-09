#include <string>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <pthread.h>
#include <algorithm> 

#include "DikeUtil.hpp"
#include "LambdaParquetReader.hpp"
#include "LambdaBinaryColumnWriter.hpp"
#include "TpchQ1.hpp"

uint8_t opLE(parquet::ByteArray * value, parquet::ByteArray * ref) {
    int len  = std::min(value->len, ref->len);
    int i;
    for ( i = 0; i < len; i++){
        if(value->ptr[i] != ref->ptr[i]) {
            break;
        }
    }
    if(i < len) { // Mismatch was found
        if(value->ptr[i] < ref->ptr[i]) {
            return 1;
        } else {
            return 0;
        }
    }

    // All compared characters match
    if (value->len  > ref->len) {        
        return 0;
    }
    return 1;
}

int TpchQ1::writeColumn(uint8_t * res, int out_rows, int64_t * int64_values)
{
    if(lambdaBinaryColumnWriter->buffer->getFreeSpace() < (out_rows + 1 ) * sizeof(int64_t)) {
        lambdaBinaryColumnWriter->buffer = lambdaBinaryColumnWriter->getBuffer();
    }
    int64_t be_value = htobe64(out_rows * sizeof(int64_t));
    lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
    int count = 0;
    int i = 0;
    while(count < out_rows) {
        if(res[i]){
            int64_t be_value = htobe64(int64_values[i]);
            lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
            count++;
        }
        i++;
    }
    return (out_rows + 1 ) * sizeof(int64_t);
}

int TpchQ1::writeColumn(uint8_t * res, int out_rows, double * double_values)
{
    if(lambdaBinaryColumnWriter->buffer->getFreeSpace() < (out_rows + 1 ) * sizeof(double)) {
        lambdaBinaryColumnWriter->buffer = lambdaBinaryColumnWriter->getBuffer();
    }
    int64_t be_value = htobe64(out_rows * sizeof(int64_t));
    lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(double));
    int count = 0;
    int i = 0;
    while(count < out_rows) {
        if(res[i]){
            int64_t be_value = htobe64(*(int64_t *)&double_values[i]);
            lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(double));
            count++;
        }
        i++;
    }
    return (out_rows + 1 ) * sizeof(double);
}

int TpchQ1::writeColumn(uint8_t * res, int out_rows, parquet::ByteArray * ba_values)
{
    if(lambdaBinaryColumnWriter->buffer->getFreeSpace() < (out_rows + sizeof(int64_t))) {
        lambdaBinaryColumnWriter->buffer = lambdaBinaryColumnWriter->getBuffer();
    }
    int64_t be_value = htobe64(out_rows);
    lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
    int count = 0;
    int i = 0;
    int total_len = 0;
    while(count < out_rows) {
        if(res[i]){
            uint8_t len = (uint8_t)ba_values[i].len;
            lambdaBinaryColumnWriter->buffer->write(&len, 1);
            total_len += len;
            count++;
        }
        i++;
    }

    if(lambdaBinaryColumnWriter->buffer->getFreeSpace() < total_len + sizeof(int64_t)) {
        lambdaBinaryColumnWriter->buffer = lambdaBinaryColumnWriter->getBuffer();
    }
    
    be_value = htobe64(total_len);
    lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
    count = 0;
    i = 0;
    while(count < out_rows) {
        if(res[i]){            
            lambdaBinaryColumnWriter->buffer->write(ba_values[i].ptr, ba_values[i].len);            
            count++;
        }
        i++;
    }    

    return out_rows + total_len + 2*sizeof(int64_t);
}


int TpchQ1::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    verbose = std::stoi(dikeProcessorConfig["system.verbose"]);

    if (verbose) {
        std::cout << "TpchQ1::Run" << std::endl;
    }

    lambdaParquetReader = new LambdaParquetReader(dikeProcessorConfig);
    lambdaBinaryColumnWriter = new LambdaBinaryColumnWriter(output);
 
    std::stringstream projectionStream(dikeProcessorConfig["Configuration.Projection"]);
    int projectionColumns[64];
    int projectionCount = 0;
    std::string colStr;
    while (std::getline(projectionStream, colStr, ',')) {
        lambdaParquetReader->initColumn(stoi(colStr));
        projectionColumns[projectionCount] = stoi(colStr);
        projectionCount++;
    }    

    std::stringstream filter(dikeProcessorConfig["Configuration.Filter"]);
    
    std::getline(filter, colStr, ' ');
    int filterCol = stoi(colStr);

    lambdaParquetReader->initColumn(filterCol); // We have to read filter column
    std::string fileterOp;
    std::getline(filter, fileterOp, ' ');
    std::string filterValue;
    std::getline(filter, filterValue, '\n');
    parquet::ByteArray baFilterValue(filterValue.length(), (const uint8_t*)filterValue.c_str());
    if (verbose) {
        std::cout << "TpchQ1 Filer " << filterValue << " Len " << filterValue.length() << std::endl;
    }

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

    std::thread writerThread = lambdaBinaryColumnWriter->startWorker();
    isRunning = true;
    uint8_t res[LambdaColumnReader::BATCH_SIZE];

    // This is our first write, so buffer should have enough space
    int64_t be_value = htobe64(projectionCount);
    if (verbose) {
        std::cout << "TpchQ1 projectionCount " << projectionCount << std::endl;
    }

    lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
    for(int i = 0; i < projectionCount; i++) {
        int col =  projectionColumns[i];
        be_value = htobe64(lambdaParquetReader->lambdaColumnReader[col]->physicalType);
        if (verbose) {
            std::cout << "TpchQ1 col " << col << " type " << lambdaParquetReader->lambdaColumnReader[col]->physicalType  << std::endl;
        }

        lambdaBinaryColumnWriter->buffer->write(&be_value, sizeof(int64_t));
    }

    while (isRunning  && lambdaParquetReader->readFully()) {
        int out_rows = 0;
        memset(res, 0, LambdaColumnReader::BATCH_SIZE);
        int in_rows = lambdaParquetReader->lambdaColumnReader[filterCol]->values_size;
        for(int i = 0; i < in_rows; i++) {
            int r;
            // There are a lot of assumptions about Q1 made here
            r = opLE(&lambdaParquetReader->lambdaColumnReader[filterCol]->ba_values[i], &baFilterValue);
            res[i] = r;
            out_rows += r;
        }

        for(int i = 0; i < projectionCount; i++) {
            int col =  projectionColumns[i];
            switch(lambdaParquetReader->lambdaColumnReader[col]->physicalType) {
                case parquet::Type::INT64:
                    writeColumn(res, out_rows, lambdaParquetReader->lambdaColumnReader[col]->int64_values);
                break;
                case parquet::Type::DOUBLE:
                    writeColumn(res, out_rows, lambdaParquetReader->lambdaColumnReader[col]->double_values);
                break;                
                case parquet::Type::BYTE_ARRAY:
                    writeColumn(res, out_rows, lambdaParquetReader->lambdaColumnReader[col]->ba_values);
                break;                
            }            
        }
    }

    if (verbose) {
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> run_time = t2 - t1;        
        std::cout << "Records " << record_counter;
        std::cout << " run_time " << run_time.count()/ 1000 << " sec" << std::endl;
        
    }

    lambdaBinaryColumnWriter->close();

    if(writerThread.joinable()){
        writerThread.join();
    }
    delete lambdaBinaryColumnWriter;
    delete lambdaParquetReader;
    return(0);
}

void TpchQ1::Worker(){ }

std::map<int, std::shared_ptr<arrow::io::HadoopFileSystem> > LambdaParquetReader::hadoopFileSystemMap;
std::map< std::string, std::shared_ptr<parquet::FileMetaData> >  LambdaParquetReader::fileMetaDataMap;
std::map< std::string, std::string > LambdaParquetReader::fileLastAccessTimeMap;