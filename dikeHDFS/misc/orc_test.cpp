#include <chrono>
#include <orc/orc-config.hh>
#include <orc/Reader.hh>
#include <orc/Exceptions.hh>
#include <orc/OrcFile.hh>


int main(int argc, char const *argv[]) 
{
    std::string path = "hdfs://dikehdfs:9000/lineitem_srg.orc";    
    orc::ReaderOptions reader_opts;
    std::unique_ptr<orc::Reader> reader;
     
    //std::unique_ptr<orc::InputStream> is = orc::readFile(path);

    //reader = orc::createReader(orc::readFile(path), reader_opts);
    reader = orc::createReader(orc::readHdfsFile(path), reader_opts);

    std::cout << "Number Of Stripes : " << reader->getNumberOfStripes() << std::endl;    
    for(int i = 0; i < reader->getNumberOfStripes(); i++) {
        std::unique_ptr<orc::StripeInformation> si = reader->getStripe(i);
        std::cout << "Stripe " << i << " has " << si->getNumberOfRows() << " rows" << std::endl;
    }

    orc::RowReaderOptions row_reader_opts;
    //std::list<uint64_t> read_cols = { 5, 6, 7, 8, 9, 10 };
    std::list<uint64_t> read_cols = { 11 };
    row_reader_opts.include(read_cols);
    std::unique_ptr<orc::RowReader> row_reader;
    row_reader = reader->createRowReader(row_reader_opts);
     
    std::unique_ptr<orc::ColumnVectorBatch> row_batch = row_reader->createRowBatch(4096);
    uint64_t total_records = 0;
    int batch_id = 0;
    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();
    while (row_reader->next(*row_batch)) {
        //std::cout << batch_id << " : " << row_batch->numElements << " elements" << std::endl;
        batch_id++;
        total_records += row_batch->numElements;
    }

    // row_reader->seekToRow(rowNumber); // rowNumber the next row the reader should return
    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> read_time = t2 - t1;
    std::cout << "total_records " << total_records << " in " << read_time.count()/ 1000 << " sec"  << std::endl;
    return 0;
}

#if 0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
gcc -g -O0 orc_test.cpp -o orc_test -lstdc++  -L/usr/local/lib -lpthread -lorc -lm -lz -lsnappy -lzstd -llz4 -lhdfspp_static -lprotobuf -lsasl2 -lcrypto
#endif


#if 0
//double field
auto *fields = dynamic_cast<orc::StructVectorBatch *>(batch.get());
auto *col0 = dynamic_cast<orc::DoubleVectorBatch *>(fields->fields[0]);
double *buffer1 = col0->data.data();

//string field
auto *col4 = dynamic_cast<orc::StringVectorBatch *>(fields->fields[4]);
char **buffer2 = col4->data.data();
long *lengths = col4->length.data();
#endif


#if 0
-- Install configuration: "RELWITHDEBINFO"
-- Installing: /usr/local/lib/libsnappy.a
-- Installing: /usr/local/lib/libz.a
-- Installing: /usr/local/lib/libzstd.a
-- Installing: /usr/local/lib/liblz4.a
-- Installing: /usr/local/lib/libprotobuf.a
-- Installing: /usr/local/lib/libprotoc.a
-- Installing: /usr/local/lib/libhdfspp_static.a
-- Installing: /usr/local/share/doc/orc/LICENSE
-- Installing: /usr/local/share/doc/orc/NOTICE
-- Installing: /usr/local/include/orc/orc-config.hh
-- Up-to-date: /usr/local/include/orc
-- Installing: /usr/local/include/orc/Int128.hh
-- Installing: /usr/local/include/orc/Exceptions.hh
-- Installing: /usr/local/include/orc/Common.hh
-- Installing: /usr/local/include/orc/MemoryPool.hh
-- Installing: /usr/local/include/orc/Vector.hh
-- Installing: /usr/local/include/orc/OrcFile.hh
-- Installing: /usr/local/include/orc/Writer.hh
-- Installing: /usr/local/include/orc/Type.hh
-- Installing: /usr/local/include/orc/Reader.hh
-- Installing: /usr/local/include/orc/ColumnPrinter.hh
-- Installing: /usr/local/include/orc/BloomFilter.hh
-- Installing: /usr/local/include/orc/Statistics.hh
-- Installing: /usr/local/lib/liborc.a
-- Installing: /usr/local/bin/orc-contents
-- Installing: /usr/local/bin/orc-metadata
-- Installing: /usr/local/bin/orc-statistics
#endif

/mnt/usb-SanDisk_Ultra_128GB/tmp/orc-1.6.9