#ifndef LAMBDA_COLUMN_HPP
#define LAMBDA_COLUMN_HPP

#include <iostream>

#include <parquet/column_reader.h>

namespace lambda {

class Column;
class Node;

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
        MAX_SIZE = 64 << 10,
        MAX_TEXT_SIZE = MAX_SIZE * 128,
    };

    public:
    int id;
    std::string name;
    Column::DataType data_type;
    int64_t * int64_values = NULL;
    double *  double_values = NULL;
    parquet::ByteArray * ba_values = NULL;
    std::string * string_values = NULL;

    //uint8_t * textBuffer = NULL; // Shadow buffer 
    //uint8_t * textBufferPtr = NULL; // Pointer to memory inside textBuffer

    uint64_t row_count = 0; // Number of valid rows in this column    
    
    int useCount = 0; // How many nodes using this column
    Node * ownerNode = NULL;
    bool initialized = false; // true means memory allocated

    Column(Node * ownerNode, int id, std::string & name, Column::DataType data_type) {
        this->ownerNode = ownerNode;
        this->id = id;
        this->name = name;
        this->data_type = data_type;
        
    }

    void Init() {        
        initialized = true;
        switch(data_type) {
            case INT64:
            int64_values = new int64_t [Column::config::MAX_SIZE];
            break;
            case DOUBLE:
            double_values = new double [Column::config::MAX_SIZE];
            break;
            case BYTE_ARRAY:
            ba_values = new parquet::ByteArray [Column::config::MAX_SIZE];
            string_values = new std::string [Column::config::MAX_SIZE];
            //textBuffer = new uint8_t[MAX_TEXT_SIZE];
            break;
            default:
            std::cout << "Uknown data_type " << data_type << std::endl;
        }
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
                    ba_reader->ReadBatch(read_size - row_count, 0, 0, &ba_values[row_count], &values_read);                    
                    //fillTextBuffer(row_count, values_read);
                    UpdateStringValues(row_count, values_read);
                    row_count += values_read;
                    //std::cout << "Read Column " << id <<  " " << name << " row_count " << row_count << std::endl;
                }
            }
            break;
        }
        //std::cout << "Read Column " << id <<  " " << name << " row_count " << row_count << std::endl;
        return row_count;
    }

    void UpdateStringValues(int offset, int len) {
        for (int i = offset; i < offset + len; i++) {
            string_values[i] = std::string((const char*)ba_values[i].ptr, ba_values[i].len);
            ba_values[i].ptr = (const uint8_t*)string_values[i].c_str();
        }
    }

    ~Column() {
        if(!initialized){
            return;
        }
        //std::cout << "Column destructor " << id <<  " " << name << std::endl;
        switch(data_type) {
        case INT64:
        delete [] int64_values;
        break;
        case DOUBLE:
        delete [] double_values;
        break;
        case BYTE_ARRAY:
        delete [] ba_values;
        delete [] string_values;
        //delete [] textBuffer;
        break;
        }        
    }

    void ApplyFilter(uint8_t * filter) {
        if(!initialized) { return; }
        //std::cout << "ApplyFilter Column  " << name << std::endl;
        switch(data_type) {
        case INT64:
        _ApplyFilter(int64_values, filter);
        break;
        case DOUBLE:        
        _ApplyFilter(double_values, filter);
        break;
        case BYTE_ARRAY:
        _ApplyFilter(ba_values, filter);        
        break;
        }
    }

    uint64_t GetHash(int row) {        
        uint64_t hash = 0;
        switch(data_type) {
        case INT64:
            hash = std::hash<double>{}(*(double *)&int64_values[row]);
            break;
        case DOUBLE:
            hash = std::hash<double>{}(double_values[row]);
            break;
        case BYTE_ARRAY:
            hash = std::hash<std::string>{}(string_values[row]);            
            break;
        }
        return hash;
    }

    void CopyRow(uint64_t dst_row, Column * src_col, uint64_t src_row) {
        switch(data_type) {
        case INT64:
            int64_values[dst_row] = src_col->int64_values[src_row];
            break;
        case DOUBLE:
            double_values[dst_row] = src_col->double_values[src_row];
            break;
        case BYTE_ARRAY:            
            ba_values[dst_row] = src_col->ba_values[src_row];
            UpdateStringValues(dst_row, 1); // This will copy data and update ba->ptr
            break;
        }
    }

    private:
    template<typename T>
    void _ApplyFilter(T values, uint8_t * filter) {
        int index = -1;
        for(int i = 0; i < row_count; i++) { // Find first hole
            if(filter[i] == 0){
                index = i;
                break;
            }
        }
        if(index == -1){ // Nothing to filter out
            return;
        }
        for(int i = index + 1; i < row_count; i++) {
            if(filter[i]) { // Valid data
                values[index] = values[i];
                index++;
            }
        }
        row_count = index;
    }
};

} // namespace lambda
#endif /* LAMBDA_COLUMN_HPP */