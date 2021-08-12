#ifndef LAMBDA_COLUMN_HPP
#define LAMBDA_COLUMN_HPP

#include <parquet/column_reader.h>

namespace lambda {

class Column;
class Column {
    public:
    enum DataType {
        BOOLEAN = 0,
        INT32 = 1,
        INT64 = 2,
        INT96 = 3,
        FLOAT = 4,
        DOUBLE = 5,
        BYTE_ARRAY = 6,
        FIXED_LEN_BYTE_ARRAY = 7,
        // Should always be last element.
        UNDEFINED = 8
    };

    enum config {
        MAX_SIZE = 4096,
    };

    public:
    int id;
    std::string name;
    Column::DataType data_type;
    int64_t * int64_values = NULL;
    double *  double_values = NULL;
    parquet::ByteArray * ba_values = NULL;    

    uint64_t row_count = 0; // Number of valid row in this column
    bool memory_owner = false; // This column must destroy it's memory
    
    int refCount = 0; // How many references do we have    

    Column(int id, std::string & name, Column::DataType data_type) {
        this->id = id;
        this->name = name;
        this->data_type = data_type;       
    }

    void Init() {
        memory_owner = true;
        switch(data_type) {
            case INT64:
            int64_values = new int64_t [MAX_SIZE];
            break;
            case DOUBLE:
            double_values = new double [MAX_SIZE];
            break;
            case BYTE_ARRAY:
            ba_values = new parquet::ByteArray [MAX_SIZE];
            break;
        }
    }

    Column * Clone()
    {
        refCount++;
        return this;
    }

    int Read(std::shared_ptr<parquet::ColumnReader> reader, int read_size) {
        int64_t values_read;
        row_count = 0;
        switch(data_type) {
            case INT64:
            {
                parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(reader.get());
                while(row_count < read_size) {
                    row_count += int64_reader->ReadBatch(read_size - row_count, 0, 0, &int64_values[row_count], &values_read);
                }                
            }
            break;
            case DOUBLE:
            {
                parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(reader.get());
                while(row_count < read_size) {
                    row_count += double_reader->ReadBatch(read_size - row_count, 0, 0, &double_values[row_count], &values_read);
                }                
            }            
            break;
            case BYTE_ARRAY:
            {
                parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(reader.get());
                while(row_count < read_size) {
                    row_count += ba_reader->ReadBatch(read_size - row_count, 0, 0, &ba_values[row_count], &values_read);
                }
            }
            break;
        }
        return 0;
    }

    ~Column() {
        if(memory_owner){
            switch(data_type) {
            case INT64:
            delete [] int64_values;
            break;
            case DOUBLE:
            delete [] double_values;
            break;
            case BYTE_ARRAY:
            delete [] ba_values;
            break;
            }
        }
    }
};

} // namespace lambda
#endif /* LAMBDA_COLUMN_HPP */