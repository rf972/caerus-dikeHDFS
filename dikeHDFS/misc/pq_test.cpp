#include <iostream>

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
    
    arrow::MemoryPool* pool_ = arrow::default_memory_pool();

    DikeReadableFile(std::string path) {        
        stream = orc::readFile(path);
        fileLength =  static_cast<uint64_t>(stream->getLength());
    }

    ~DikeReadableFile() override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;      
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
      //std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;
      stream->read(out, nbytes, position);      
      return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        if(position + nbytes > fileLength){
            std::cout << " Invalid read size " << nbytes << std::endl;
        }
        //std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;        
        ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
        stream->read( buffer->mutable_data(), nbytes, position);
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
      std::cout << __FUNCTION__ << " : " << __LINE__ << " return " << fileLength << std::endl;
      return fileLength;
    }
};


int main(int argc, char ** argv)
{
    arrow::Status st;    

#if 0
    // https://gist.github.com/hurdad/06058a22ca2b56e25d63aaa6f3a9108f
	arrow::io::HdfsConnectionConfig conf;
	conf.host = "dikehdfs";		
    conf.port = 9000;
    conf.user = "peter";

	std::shared_ptr<arrow::io::HadoopFileSystem> fs;
	st = arrow::io::HadoopFileSystem::Connect(&conf, &fs);
	std::cout << st.ToString() << std::endl;
#endif

    //std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
    //st = fs->OpenReadable("/lineitem.parquet", &inputFile);
    //std::cout << st.ToString() << std::endl;
    std::shared_ptr<DikeReadableFile> inputFile = 
    //std::shared_ptr<DikeReadableFile>(new DikeReadableFile("hdfs://dikehdfs:9000/lineitem.parquet"));
    std::shared_ptr<DikeReadableFile>(new DikeReadableFile("hdfs://dikehdfs:9000/lineitem_srg.parquet"));
    
    std::shared_ptr<parquet::FileMetaData> fileMetaData;    
    fileMetaData = parquet::ReadMetaData(inputFile);

    std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
    auto schema = fileMetaData->schema();

    for(auto i = 0; i < schema->num_columns(); i++) {
        auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(i);        
        std::cout << col->name() << ",";
    }
    std::cout << std::endl;

    parquet::ReaderProperties readerProperties = parquet::default_reader_properties();
    //readerProperties.enable_buffered_stream();
    readerProperties.set_buffer_size(1<<20);
    std::cout << "Buffered stream " << readerProperties.is_buffered_stream_enabled() << " " << readerProperties.buffer_size() << std::endl;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader = parquet::ParquetFileReader::Open(inputFile, readerProperties, fileMetaData);
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader = parquetFileReader->RowGroup(0);
    const parquet::RowGroupMetaData* rowGroupMetaData = rowGroupReader->metadata();
    int colId;
    for(colId = 0; colId < rowGroupMetaData->num_columns(); colId++){
        auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(colId);       

        parquet::Type::type physicalType = col->physical_type();
        switch (physicalType) {
            case parquet::Type::BOOLEAN:
            case parquet::Type::INT32:
            break;
            
            case parquet::Type::FLOAT:
            case parquet::Type::DOUBLE:
            break;
            
            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            case parquet::Type::BYTE_ARRAY:        
            break;
            
            case parquet::Type::INT96:
            // This type exists to store timestamps in nanoseconds due to legacy
            // reasons. We just interpret it as a timestamp in milliseconds.
            case parquet::Type::INT64:
                
                break;        
            default:
                std::cout << "Uknown type "  << parquet::TypeToString(physicalType) << std::endl;
            break;      
        }    

        std::cout << "Column " << colId << " type is " << parquet::TypeToString(physicalType) << std::endl;
    }

    colId = 0;
    auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(colId);
    parquet::Type::type physicalType = col->physical_type();
    std::shared_ptr<parquet::ColumnReader> columnReader = rowGroupReader->Column(colId);
    std::cout << "Column " << colId << " type is " << parquet::TypeToString(physicalType) << std::endl;
    parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(columnReader.get());
    int64_t values_read = 0;
    int64_t rows_read = 0;
    int16_t definition_level;
    int16_t repetition_level;
    int rowCount = 0;
    while (int64_reader->HasNext()) {
        int64_t value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        //rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        rows_read = int64_reader->ReadBatch(1, NULL, NULL, &value, &values_read);
        rowCount++;
    }

    std::cout << "rowCount " << rowCount << " out of " << rowGroupMetaData->num_rows() <<std::endl;

    colId = 11;
    col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(colId);
    physicalType = col->physical_type();
    columnReader = rowGroupReader->Column(colId);
    parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(columnReader.get());

    std::cout << "Column " << colId << " type is " << parquet::TypeToString(physicalType) << std::endl;
    rowCount = 0;
    while (ba_reader->HasNext()) {
        parquet::ByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        //rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        rows_read = ba_reader->ReadBatch(1, NULL, NULL, &value, &values_read);
        
        if(rowCount < 10){
            std::cout << std::string((const char *)value.ptr, value.len) << std::endl;
            //std::cout << value.len << std::endl;
        }
        rowCount++;
    }

    std::cout << "rowCount " << rowCount << " out of " << rowGroupMetaData->num_rows() <<std::endl;    
    return 0;
}

#if 0
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath) 

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
gcc -g -O0 pq_test.cpp -o pq_test -lstdc++  -lpthread -lm -larrow -llz4 -lparquet -L/usr/local/lib -lorc -lm -lz -lhdfspp_static -lprotobuf -lsasl2 -lcrypto
// -lsnappy -lzstd -llz4
#endif
