#include "LambdaFilterNode.hpp"

#include <array>
#include <string>

using namespace lambda;

namespace lambda {

class Filter;

template<typename T>
void LessThanOrEqual(T * column, T value, int len, uint8_t * result) {
    for(int c = 0; c < len; c++) {
        if(result[c]) { // Skip zeroes in result vector
            if(!(column[c] <= value)) { result[c] = 0;}
        }
    }
}

template<typename T>
void LessThan(T * column, T value, int len, uint8_t * result) {
    for(int c = 0; c < len; c++) {
        if(result[c]) { // Skip zeroes in result vector
            if(!(column[c] < value)) { result[c] = 0;}
        }
    }
}

template<typename T>
void GreaterThanOrEqual(T * column, T value, int len, uint8_t * result) {
    for(int c = 0; c < len; c++) {
        if(result[c]) { // Skip zeroes in result vector
            if(!(column[c] >= value)) { result[c] = 0;}
        }
    }
}

template<typename T>
void GreaterThan(T * column, T value, int len, uint8_t * result) {
    for(int c = 0; c < len; c++) {
        if(result[c]) { // Skip zeroes in result vector
            if(!(column[c] > value)) { result[c] = 0;}
        }
    }
}

template<typename T>
void EqualTo(T * column, T value, int len, uint8_t * result) {
    for(int c = 0; c < len; c++) {
        if(result[c]) { // Skip zeroes in result vector
            if(!(column[c] == value)) { result[c] = 0;}
        }
    }
}

void LessThanOrEqual(Column * column, std::string & value, uint8_t * result) {    
    int i;
    parquet::ByteArray ba_value;
    ba_value.ptr = (const uint8_t*)value.c_str();
    ba_value.len = value.length();    
    for(int c = 0; c < column->row_count; c++) {
        if(result[c]) { // Skip zeroes in result vector
            parquet::ByteArray * ref = &column->ba_values[c];
            int len  = std::min(ba_value.len, ref->len);
            for ( i = 0; i < len; i++){
                if(ba_value.ptr[i] != ref->ptr[i]) { break; }
            }
            if(i < len) { // Data mismatch was found
                if(!(ref->ptr[i] <= ba_value.ptr[i])) { result[c] = 0; }
            } else { // All compared characters match 
                if (!(ref->len <= ba_value.len)) {  // Size mismatch 
                    result[c] = 0;
                }
            }
        }
    } // Loop over all rows
}

void LessThan(Column * column, std::string & value, uint8_t * result) {    
    int i;
    parquet::ByteArray ba_value;
    ba_value.ptr = (const uint8_t*)value.c_str();
    ba_value.len = value.length();    
    for(int c = 0; c < column->row_count; c++) {
        if(result[c]) { // Skip zeroes in result vector
            parquet::ByteArray * ref = &column->ba_values[c];
            int len  = std::min(ba_value.len, ref->len);
            for ( i = 0; i < len; i++){
                if(ba_value.ptr[i] != ref->ptr[i]) { break; }
            }
            if(i < len) { // Data mismatch was found
                if(!(ref->ptr[i] < ba_value.ptr[i])) { result[c] = 0; }
            } else { // All compared characters match 
                if (!(ref->len < ba_value.len)) {  result[c] = 0; } // Size mismatch 
            }
        }
    } // Loop over all rows
}

void GreaterThanOrEqual(Column * column, std::string & value, uint8_t * result) {    
    int i;
    parquet::ByteArray ba_value;
    ba_value.ptr = (const uint8_t*)value.c_str();
    ba_value.len = value.length();    
    for(int c = 0; c < column->row_count; c++) {
        if(result[c]) { // Skip zeroes in result vector
            parquet::ByteArray * ref = &column->ba_values[c];
            int len  = std::min(ba_value.len, ref->len);
            for ( i = 0; i < len; i++){
                if(ba_value.ptr[i] != ref->ptr[i]) { break; }
            }
            if(i < len) { // Data mismatch was found
                if(!(ref->ptr[i] >= ba_value.ptr[i])) { result[c] = 0; }
            } else { // All compared characters match 
                if (!(ref->len >= ba_value.len)) {  // Size mismatch 
                    result[c] = 0;
                }
            }
        }
    } // Loop over all rows
}

void GreaterThan(Column * column, std::string & value, uint8_t * result) {    
    int i;
    parquet::ByteArray ba_value;
    ba_value.ptr = (const uint8_t*)value.c_str();
    ba_value.len = value.length();    
    for(int c = 0; c < column->row_count; c++) {
        if(result[c]) { // Skip zeroes in result vector
            parquet::ByteArray * ref = &column->ba_values[c];
            int len  = std::min(ba_value.len, ref->len);
            for ( i = 0; i < len; i++){
                if(ba_value.ptr[i] != ref->ptr[i]) { break; }
            }
            if(i < len) { // Data mismatch was found
                if(!(ref->ptr[i] > ba_value.ptr[i])) { result[c] = 0; }
            } else { // All compared characters match 
                if (!(ref->len > ba_value.len)) {  result[c] = 0; } // Size mismatch 
            }
        }
    } // Loop over all rows
}

void EqualTo(Column * column, std::string & value, uint8_t * result) {    
    int i;
    parquet::ByteArray ba_value;
    ba_value.ptr = (const uint8_t*)value.c_str();
    ba_value.len = value.length();    
    for(int c = 0; c < column->row_count; c++) {
        if(result[c]) { // Skip zeroes in result vector
            parquet::ByteArray * ref = &column->ba_values[c];
            int len  = std::min(ba_value.len, ref->len);
            for ( i = 0; i < len; i++){
                if(ba_value.ptr[i] != ref->ptr[i]) { break; }
            }
            if(i < len) { // Data mismatch was found
               result[c] = 0;
            } else { // All compared characters match 
                if (ref->len != ba_value.len) {  result[c] = 0; } // Size mismatch 
            }
        }
    } // Loop over all rows
}

class Filter {
    public:
    int verbose = 0;
    
    Column::DataType data_type;
    int64_t int64_value = 0;
    double double_value = 0;

    std::array<std::string, 2> columnNames;
    std::array<std::string, 2> values;
    int columnMap[2] = {-1, -1};
    enum {
        LEFT = 0,
        RIGHT = 1,
    };

    enum FilterExpression {
        _LE = 1, // LessThanOrEqual
        _LT = 2, // LessThan
        _GE = 3, // GreaterThanOrEqual
        _GT = 4, // GreaterThan
        _EQ = 5, // EqualTo
    };

    int expression = 0;

    Filter(Poco::JSON::Object::Ptr pObject, int verbose) {
        this->verbose = verbose;
        std::string expr = pObject->getValue<std::string>("Expression");
        if(expr.compare("LessThanOrEqual") == 0){
            expression = _LE;
        } else if(expr.compare("LessThan") == 0){
            expression = _LT;
        } else if(expr.compare("GreaterThanOrEqual") == 0){
            expression = _GE;
        }else if(expr.compare("GreaterThan") == 0){
            expression = _GT;
        } else if(expr.compare("EqualTo") == 0){
            expression = _EQ;
        } else {
            std::cout << "Uknown expression : " << expr << std::endl;
        }

        Poco::JSON::Object::Ptr side;
        side = pObject->getObject("Left");
        if(side->has("Literal")) {
            values[LEFT] = side->getValue<std::string>("Literal");   
        } else if(side->has("ColumnReference")) {
            columnNames[LEFT] = side->getValue<std::string>("ColumnReference");
        }    

        side = pObject->getObject("Right");
        if(side->has("Literal")) {
            values[RIGHT] = side->getValue<std::string>("Literal");   
        } else if(side->has("ColumnReference")) {
            columnNames[RIGHT] = side->getValue<std::string>("ColumnReference");
        }

        if(verbose){
            std::cout << "Filter ";
            if(columnNames[LEFT].length() > 0){
                std::cout << columnNames[LEFT] + " ";
            } else {
                std::cout << values[LEFT] << " ";
            }
            std::cout << expr << " ";
            if(columnNames[RIGHT].length() > 0){
                std::cout << columnNames[RIGHT] + " ";
            } else {
                std::cout << values[RIGHT] << " ";
            }
             std::cout << std::endl;
        }
    }

    void UpdateColumnMap(Frame * inFrame) {
        for(int i = 0; i < inFrame->columns.size(); i++){
            for(int j = 0; j < 2; j++){
                if(inFrame->columns[i]->name.compare(columnNames[j]) == 0) {
                    inFrame->columns[i]->useCount++;
                    columnMap[j] = i;
                    UpdateDataType( inFrame->columns[i]->data_type );
                }
            }
        }
    }

    void UpdateDataType(Column::DataType data_type) {
        this->data_type = data_type;
        switch(data_type) {
            case Column::DataType::INT64:
            int64_value = std::stoll(values[RIGHT]); // Other sides not supported yet
            break;
            case Column::DataType::DOUBLE:
            double_value = std::stod(values[RIGHT]); // Other sides not supported yet
            break;
            case Column::DataType::BYTE_ARRAY:
            break;
            default:
            std::cout << "Uknown data_type " << data_type << std::endl;
        }        
    }

    void Step(Frame * inFrame, uint8_t * result) {
        switch(data_type) {
            case Column::DataType::INT64:            
            Step(inFrame, int64_value, result);
            break;
            case Column::DataType::DOUBLE:            
            Step(inFrame, double_value, result);
            break;
            case Column::DataType::BYTE_ARRAY:
            Step(inFrame, values[RIGHT], result); // Other sides not supported yet
            break;
            default:
            std::cout << "Uknown data_type " << data_type << std::endl;
        }        
    }

    void Step(Frame * inFrame, std::string & value, uint8_t * result) {
        switch(expression){
            case _LE:
            LessThanOrEqual(inFrame->columns[columnMap[LEFT]], value, result); // Other sides not supported yet
            break;
            case _LT:
            LessThan(inFrame->columns[columnMap[LEFT]], value, result); // Other sides not supported yet
            break;
            case _GE:
            GreaterThanOrEqual(inFrame->columns[columnMap[LEFT]], value, result); // Other sides not supported yet
            break;
            case _GT:
            GreaterThan(inFrame->columns[columnMap[LEFT]], value, result); // Other sides not supported yet
            break;            
            case _EQ:
            EqualTo(inFrame->columns[columnMap[LEFT]], value, result); // Other sides not supported yet
            break;            

        }
    }
    void Step(Frame * inFrame, double value, uint8_t * result) {
        double * data = inFrame->columns[columnMap[LEFT]]->double_values;
        int len = inFrame->columns[columnMap[LEFT]]->row_count;
        switch(expression){
            case _LE:
            LessThanOrEqual(data, value, len, result); 
            break;
            case _LT:
            LessThan(data, value, len, result); 
            break;
            case _GE:
            GreaterThanOrEqual(data, value, len, result); 
            break;
            case _GT:
            GreaterThan(data, value, len, result); 
            break;            
            case _EQ:
            EqualTo(data, value, len, result); 
            break;     
        }
    }
    void Step(Frame * inFrame, int64_t value, uint8_t * result) {
        int64_t * data = inFrame->columns[columnMap[LEFT]]->int64_values;
        int len = inFrame->columns[columnMap[LEFT]]->row_count;
        switch(expression){
            case _LE:
            LessThanOrEqual(data, value, len, result); 
            break;
            case _LT:
            LessThan(data, value, len, result); 
            break;
            case _GE:
            GreaterThanOrEqual(data, value, len, result); 
            break;
            case _GT:
            GreaterThan(data, value, len, result); 
            break;
            case _EQ:
            EqualTo(data, value, len, result); 
            break;     
        }
    }    
};
} // namespace lambda


FilterNode::FilterNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
: Node(pObject, dikeProcessorConfig, output) 
{
    Poco::JSON::Array::Ptr array = pObject->getArray("FilterArray");
    for(int i = 0; i < array->size(); i++) {
        Poco::JSON::Object::Ptr filter = array->getObject(i);
        Poco::JSON::Object::Ptr rightSide;
        Poco::JSON::Object::Ptr leftSide;
        std::string expression = filter->getValue<std::string>("Expression");
        if(expression.compare("IsNotNull") == 0){
            // Skip it for now
        } else  {
            filterArray.push_back(new Filter(filter, verbose));
        }
    }
    result = new uint8_t [Column::config::MAX_SIZE];
}

FilterNode::~FilterNode()
{
    for(int i = 0; i < filterArray.size(); i++) {
        delete filterArray[i];
    }
    delete [] result;
}

void FilterNode::UpdateColumnMap(Frame * inFrame) 
{
    for(int i = 0; i < filterArray.size(); i++){
        filterArray[i]->UpdateColumnMap(inFrame);
    }
    Node::UpdateColumnMap(inFrame);
}

bool FilterNode::Step()
{
    //std::cout << "FilterNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    Frame * inFrame = getFrame();    
    if(inFrame == NULL) {
        std::cout << "Input queue is empty " << std::endl;
        return done;
    }

    memset(result, 1,  Column::config::MAX_SIZE);

    for(int i = 0; i < filterArray.size(); i++){
        filterArray[i]->Step(inFrame, result);
    }

    inFrame->ApplyFilter(result);

    if(inFrame->lastFrame){        
        done = true;
    }

    nextNode->putFrame(inFrame); // Send frame down to graph    
    return done;
}