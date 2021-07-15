#include <iostream>

#include<iostream>
#include<fstream>

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>  // IWYU pragma: keep
#include <parquet/schema.h>
#include <parquet/properties.h>
#include <parquet/column_reader.h>
#include <parquet/api/writer.h>


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>

#include <arrow/filesystem/filesystem.h>

#include <arrow-glib/output-stream.h>

// c_glib/parquet-glib/arrow-file-writer.cpp
//#include <arrow-glib/arrow-glib.hpp>
//#include <parquet-glib/arrow-file-writer.hpp>

class MyOutputStream : public ::arrow::io::OutputStream /* ::arrow::io::FileOutputStream */ {    
    public:
    int64_t pos = 0;
    std::ofstream wf;

    MyOutputStream(const std::string& name)  {
       wf.open(name, std::ios::out | std::ios::binary);
    }

    virtual ~MyOutputStream() {
        std::cout << __func__ << std::endl;
    }

    /// \brief Write the given data to the stream
    ///
    /// This method always processes the bytes in full.  Depending on the
    /// semantics of the stream, the data may be written out immediately,
    /// held in a buffer, or written asynchronously.  In the case where
    /// the stream buffers the data, it will be copied.  To avoid potentially
    /// large copies, use the Write variant that takes an owned Buffer.
    virtual arrow::Status Write(const void* data, int64_t nbytes) override {
        std::cout << __func__ << ":" << nbytes << std::endl;
        wf.write((const char*)data, nbytes);
        pos += nbytes;
        return arrow::Status::OK();
    }

    /// \brief Write the given data to the stream
    ///
    /// Since the Buffer owns its memory, this method can avoid a copy if
    /// buffering is required.  See Write(const void*, int64_t) for details.
    virtual arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override {
        //std::cout << __func__ << " # " << __LINE__<< std::endl;
        //return arrow::Status::OK();
        return this->Write((const void*) data->data(), (int64_t) data->size());
    }
    /// \brief Flush buffered bytes, if any
    virtual arrow::Status Flush() override {
        std::cout << __func__ << std::endl;
        return arrow::Status::OK();
    }

    virtual arrow::Status Close() override {
        std::cout << __func__ << " at pos " << pos << std::endl;
        wf.close();
        return arrow::Status::OK();
    }

    virtual bool closed() const override {
        std::cout << __func__ << std::endl;
        return false;        
    }

    // Return the position in this stream.
    virtual arrow::Result<int64_t> Tell() const override {
        std::cout << __func__ << " : " << pos << std::endl;
        return pos;        
    }
};

int main(int argc, char ** argv)
{   
    using namespace parquet; 
    using namespace parquet::schema;    

    // Create Schema
    NodeVector fields;
    fields.push_back(PrimitiveNode::Make("COUNT(*)", Repetition::REQUIRED, Type::INT64, ConvertedType::NONE));
    //fields.push_back(schema::PrimitiveNode::Make("l_extendedprice", Repetition::REQUIRED, Type::DOUBLE, ConvertedType::NONE));

    std::shared_ptr<GroupNode> schema = 
        std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> writerProperties = builder.build();

    std::shared_ptr<MyOutputStream> out_file(new MyOutputStream("pq_write.parquet"));    
    //std::shared_ptr<::arrow::io::FileOutputStream> out_file = ::arrow::io::FileOutputStream::Open("pq_write.parquet").ValueOrDie();

   // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, writerProperties);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer = file_writer->AppendBufferedRowGroup();
    
    //std::cout << rg_writer->current_column() << std::endl;
    void * cl_writers[2];

    cl_writers[0] = rg_writer->column(0);
    //cl_writers[1] = rg_writer->column(1);

    //parquet::Int64Writer* int64_writer = static_cast<parquet::Int64Writer*>(cl_writers[0]);
    //parquet::DoubleWriter* double_writer = static_cast<parquet::DoubleWriter*>(cl_writers[1]);

    //std::cout << rg_writer->current_column() << std::endl;
#if 0
    for (int i = 0; i < 10; i++) {
        std::cout << "Row " << i << std::endl;
        int64_t int64_value = i;
        //static_cast<parquet::Int64Writer*>(cl_writers[0])->WriteBatch(1, nullptr, nullptr, &int64_value);
        int64_writer->WriteBatch(1, nullptr, nullptr, &int64_value);
        double double_value = i;
        static_cast<parquet::DoubleWriter*>(cl_writers[1])->WriteBatch(1, nullptr, nullptr, &double_value);
        //double_writer->WriteBatch(1, nullptr, nullptr, &double_value);
    }
#endif
    for (int i = 0; i < 1000; i++) {
        int64_t int64_value = i;
        static_cast<parquet::Int64Writer*>(cl_writers[0])->WriteBatch(1, nullptr, nullptr, &int64_value);
    }

    rg_writer->Close();
    rg_writer = file_writer->AppendBufferedRowGroup();
    cl_writers[0] = rg_writer->column(0);

    for (int i = 0; i < 1000; i++) {
        int64_t int64_value = i;
        static_cast<parquet::Int64Writer*>(cl_writers[0])->WriteBatch(1, nullptr, nullptr, &int64_value);
    }

    rg_writer->Close();
    
    // Close the ParquetFileWriter
    file_writer->Close();
    out_file->Close();

    return 0;
}

#if 0
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath) 
gcc -I /usr/include/glib-2.0 -I /usr/lib/x86_64-linux-gnu/glib-2.0/include -g -O0 pq_write.cpp -o pq_write -lstdc++  -lpthread -lm -larrow -lparquet

/opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -put -f pq_write.parquet /
#endif



#if 0
      "firstDataPageOffset" : 523101,
      "type" : "INT64",
      "path" : [ "l_orderkey" ],
      "encodings" : [ "PLAIN_DICTIONARY", "PLAIN", "RLE" ],
      "startingPos" : 4,
      "codec" : "SNAPPY",
      "primitiveType" : {
        "name" : "l_orderkey",
        "repetition" : "OPTIONAL",
        "logicalTypeAnnotation" : null,
        "id" : null,
        "primitive" : true,
        "primitiveTypeName" : "INT64",
        "typeLength" : 0,
        "decimalMetadata" : null,
        "originalType" : null
      }
    }, {
#endif