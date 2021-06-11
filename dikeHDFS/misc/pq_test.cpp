#include <iostream>

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/properties.h>
#include <parquet/column_reader.h>


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/filesystem/filesystem.h>




int main(int argc, char ** argv)
{
    arrow::Status st;    
#if 0
    std::shared_ptr<arrow::fs::FileSystem> fs;
    fs = arrow::fs::FileSystemFromUri("file:///data").ValueOrDie();

    std::shared_ptr<arrow::io::RandomAccessFile> inputFile;
    inputFile = fs->OpenInputFile("/data/lineitem.parquet").ValueOrDie();
#endif

    // Open Parquet file reader
    //std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
    std::shared_ptr<arrow::Table> table;

    // https://gist.github.com/hurdad/06058a22ca2b56e25d63aaa6f3a9108f
	arrow::io::HdfsConnectionConfig conf;
	conf.host = "dikehdfs";	
	//conf.port = 8020;
    conf.port = 9000;
    conf.user = "peter";

	std::shared_ptr<arrow::io::HadoopFileSystem> fs;
	st = arrow::io::HadoopFileSystem::Connect(&conf, &fs);
	std::cout << st.ToString() << std::endl;

    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
    st = fs->OpenReadable("/lineitem.parquet", &inputFile);
    std::cout << st.ToString() << std::endl;

    std::shared_ptr<parquet::FileMetaData> fileMetaData;    
    fileMetaData = parquet::ReadMetaData(inputFile);

    std::cout << "Succesfully read fileMetaData " << fileMetaData->num_rows() << " rows in " << fileMetaData->num_row_groups() << " RowGroups" << std::endl;
    auto schema = fileMetaData->schema();

    for(auto i = 0; i < schema->num_columns(); i++) {
        auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(i);
        
        std::cout << col->name() << ",";
    }
    std::cout << std::endl;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader = parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData);
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader = parquetFileReader->RowGroup(0);
    const parquet::RowGroupMetaData* rowGroupMetaData = rowGroupReader->metadata();

    int colId = 0;
    auto col = (parquet::schema::PrimitiveNode*)schema->GetColumnRoot(colId);
    std::shared_ptr<parquet::ColumnReader> columnReader = rowGroupReader->Column(colId);    

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

    std::cout << "Column 0 type is " << parquet::TypeToString(physicalType) << std::endl;
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

#if 0    
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

#endif
    
    return 0;
}

#if 0
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath) 
gcc -g -O0 pq_test.cpp -o pq_test -lstdc++  -lpthread -lm -larrow -lparquet
#endif
