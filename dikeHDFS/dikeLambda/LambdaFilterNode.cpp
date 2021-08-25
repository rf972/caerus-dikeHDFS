#include "LambdaFilterNode.hpp"

using namespace lambda;

FilterNode::FilterNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
: Node(pObject, dikeProcessorConfig, output) 
{
    Poco::JSON::Array::Ptr filterArray = pObject->getArray("FilterArray");
    for(int i = 0; i < filterArray->size(); i++) {
        Poco::JSON::Object::Ptr filter = filterArray->getObject(i);
        Poco::JSON::Object::Ptr rightSide;
        Poco::JSON::Object::Ptr leftSide;
        std::string expression = filter->getValue<std::string>("Expression");
        if(verbose){
            std::cout << "Filter[" << i << "] Expression : " << expression << std::endl;
        }
        if(expression.compare("LessThanOrEqual") == 0){
            rightSide = filter->getObject("Right");
            if(rightSide->has("ColumnReference")) {
                std::cout << "Filter[" << i << "] ColumnReference : " << rightSide->getValue<std::string>("ColumnReference") << std::endl;
            } else if(rightSide->has("Literal")) {
                std::cout << "Filter[" << i << "] Literal : " << rightSide->getValue<std::string>("Literal") << std::endl;
            }

            leftSide = filter->getObject("Left");
            if(leftSide->has("ColumnReference")) {
                std::cout << "Filter[" << i << "] ColumnReference : " << leftSide->getValue<std::string>("ColumnReference") << std::endl;
            } else if(leftSide->has("Literal")) {
                std::cout << "Filter[" << i << "] Literal : " << leftSide->getValue<std::string>("Literal") << std::endl;
            }

        }
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