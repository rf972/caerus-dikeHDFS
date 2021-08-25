#include "LambdaFilterNode.hpp"

#include <array>

using namespace lambda;

namespace lambda {

class Filter;
//typedef int (Filter::*MyTypedef)( int); //MyTypedef is a type!    

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

class Filter {
    public:
    int verbose = 0;
    
    std::array<std::string, 2> columnNames;
    std::array<std::string, 2> values;
    int columnMap[2] = {-1, -1};
    enum {
        LEFT = 0,
        RIGHT = 1,
    };

    enum FilterExpression {
        _LE = 1,
    };

    int expression = 0;

    Filter(Poco::JSON::Object::Ptr pObject, int verbose) {
        this->verbose = verbose;
        std::string expr = pObject->getValue<std::string>("Expression");
        if(expr.compare("LessThanOrEqual") == 0){
            expression = _LE;
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
                }
            }
        }
    }

    void Step(Frame * inFrame, uint8_t * result) {
        switch(expression){
            case _LE:
            LessThanOrEqual(inFrame->columns[columnMap[LEFT]], values[RIGHT], result); // Other sides not supported yet
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
        if(verbose){
            std::cout << "Filter[" << i << "] Expression : " << expression << std::endl;
        }
        if(expression.compare("LessThanOrEqual") == 0){
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