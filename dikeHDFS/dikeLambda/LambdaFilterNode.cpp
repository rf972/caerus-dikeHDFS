#include "LambdaFilterNode.hpp"

using namespace lambda;

namespace lambda {
class Filter {
    public:
    int verbose = 0;
    std::string valueStr;
    std::vector<std::string> columnNames;

    Filter(Poco::JSON::Object::Ptr pObject, int verbose) {
        this->verbose = verbose;
        std::string expression = pObject->getValue<std::string>("Expression");
        Poco::JSON::Object::Ptr side;
        side = pObject->getObject("Right");
        if(side->has("Literal")) {
            valueStr = side->getValue<std::string>("Literal");
        } else if(side->has("ColumnReference")) {
            columnNames.push_back(side->getValue<std::string>("ColumnReference"));
        }

        side = pObject->getObject("Left");
        if(side->has("Literal")) {
            valueStr = side->getValue<std::string>("Literal");
            if(verbose) {
                std::cout << "Filter " << valueStr << " " << expression << " " << columnNames[0] << std::endl;
            }            
        } else if(side->has("ColumnReference")) {
            columnNames.push_back(side->getValue<std::string>("ColumnReference"));
            if(verbose) {
                std::cout << "Filter " << columnNames[0] << " " << expression << " " << valueStr << std::endl;
            }            
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
}

FilterNode::~FilterNode()
{
    for(int i = 0; i < filterArray.size(); i++) {
        delete filterArray[i];
    }
}

void FilterNode::UpdateColumnMap(Frame * inFrame) 
{
    Node::UpdateColumnMap(inFrame);
}

bool FilterNode::Step()
{
    Frame * inFrame = getFrame();    
    if(inFrame == NULL) {
        std::cout << "Input queue is empty " << std::endl;
        return done;
    }

    if(inFrame->lastFrame){        
        done = true;
    }

    nextNode->putFrame(inFrame); // Send frame down to graph
    
    return done;
}