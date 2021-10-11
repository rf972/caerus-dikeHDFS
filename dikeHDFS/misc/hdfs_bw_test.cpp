#include <iostream>
#include <thread>
#include <vector>

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/properties.h>
#include <parquet/column_reader.h>


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/filesystem/filesystem.h>

#include <orc/orc-config.hh>
#include <orc/Reader.hh>
#include <orc/Exceptions.hh>
#include <orc/OrcFile.hh>

class DikeReadableFile : public arrow::io::RandomAccessFile {    
 public:
    std::shared_ptr<arrow::Buffer> buffer;
   
    std::unique_ptr<orc::InputStream> stream;
    uint64_t fileLength;
    uint64_t totalBytesRead = 0;;
    
    arrow::MemoryPool* pool_ = arrow::default_memory_pool();

    DikeReadableFile(std::string path) {        
        stream = orc::readFile(path);
        fileLength =  static_cast<uint64_t>(stream->getLength());
    }

    ~DikeReadableFile() override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;      
    }

    arrow::Status Close() override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return arrow::Status::OK();
    }

    bool closed() const override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return false;
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return 0;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return std::move(buffer);
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;
      stream->read(out, nbytes, position);
      totalBytesRead += nbytes;
      return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        if(position + nbytes > fileLength){
            std::cout << " Invalid read size " << nbytes << std::endl;
        }
        // std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;        
        ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
        stream->read( buffer->mutable_data(), nbytes, position);
        totalBytesRead += nbytes;
        return std::move(buffer);         
    }

    arrow::Status Seek(int64_t position) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return 0;
    }

    arrow::Result<int64_t> GetSize() override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << " return " << fileLength << std::endl;
      return fileLength;
    }
};

void readerThread(uint64_t  arg);

int main(int argc, char ** argv)
{   
    int tCount = std::stoi(argv[1]);
    std::vector<std::thread> threads(32);

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i < tCount; i++) {
        threads[i] = std::thread(readerThread, i );
    }

    for (uint64_t i = 0; i < tCount; i++) {
       threads[i].join();
    }
    
    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> run_time = t2 - t1;

    std::cout << "Actual run_time " << run_time.count()/ 1000 << " sec" << std::endl;
    std::cout << "Actual BW " << ( 35 * tCount ) / (run_time.count() / 1000) << " MB/s" << std::endl; 
    return 0;
}

void readerThread(uint64_t  arg) 
{
    uint64_t rowGroupId = (uint64_t) arg;
    //std::cout << "rowGroupId " << rowGroupId << std::endl;
    std::shared_ptr<DikeReadableFile> inputFile =     
    std::shared_ptr<DikeReadableFile>(new DikeReadableFile("hdfs://dikehdfs:9000/tpch-test-parquet/lineitem.parquet"));
    
    std::shared_ptr<parquet::FileMetaData> fileMetaData;    
    fileMetaData = parquet::ReadMetaData(inputFile);

    //std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
    auto schema = fileMetaData->schema();
    parquet::ReaderProperties readerProperties = parquet::default_reader_properties();
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader = parquet::ParquetFileReader::Open(inputFile, readerProperties, fileMetaData);
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader = parquetFileReader->RowGroup(rowGroupId);
    const parquet::RowGroupMetaData* rowGroupMetaData = rowGroupReader->metadata();    
     
    // _5, _6, _7, _8, _9, _10, _11
    std::shared_ptr<parquet::ColumnReader> columnReader[16];
    for(int i = 0; i < 7; i++){
        columnReader[i] = rowGroupReader->Column(i+5);
    }

    //std::cout << "totalBytesRead " << inputFile->totalBytesRead << std::endl;    
}

#if 0
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath) 

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
gcc -g -O0 hdfs_bw_test.cpp -o hdfs_bw_test -lstdc++  -lpthread -lm -larrow -llz4 -lparquet -L/usr/local/lib -lorc -lm -lz -lhdfspp_static -lprotobuf -lsasl2 -lcrypto -lsnappy -lzstd -llz4
#endif
