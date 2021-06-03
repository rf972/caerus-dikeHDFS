#include <iostream>

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/properties.h>

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

    
    return 0;
}

// export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
// export CLASSPATH=$(bin/hadoop classpath) 
// gcc pq_test.cpp -o pq_test -lstdc++  -lpthread -lm -larrow -lparquet