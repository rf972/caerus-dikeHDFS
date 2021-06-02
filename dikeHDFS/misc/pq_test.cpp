#include <iostream>

#include <parquet/arrow/reader.h>

#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/filesystem/filesystem.h>


int main(int argc, char ** argv)
{
   // ...
   arrow::Status st;
   arrow::MemoryPool* pool = arrow::default_memory_pool();

   std::shared_ptr<arrow::fs::FileSystem> fs;
   fs = arrow::fs::FileSystemFromUri("file:///data").ValueOrDie();

   std::shared_ptr<arrow::io::RandomAccessFile> inputFile;
   inputFile = fs->OpenInputFile("/data/lineitem.parquet").ValueOrDie();

   // Open Parquet file reader
   std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
   st = parquet::arrow::OpenFile(inputFile, pool, &arrow_reader);
   if (!st.ok()) {      
      std::cout << "Handle error instantiating file reader... " << std::endl;
   }

   // Read entire file as a single Arrow table
   std::shared_ptr<arrow::Table> table;
   st = arrow_reader->ReadTable(&table);
   if (!st.ok()) {
      std::cout << "Handle error reading Parquet data... "  << std::endl;
   }

   std::cout << "Succesfully read " << table->num_rows() << " rows" << std::endl;
   return 0;
}

// gcc pq_test.cpp -o pq_test -lstdc++  -lpthread -lm -larrow -lparquet