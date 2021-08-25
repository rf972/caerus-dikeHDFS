#include "LambdaFilterNode.hpp"

using namespace lambda;

FilterNode::FilterNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
: Node(pObject, dikeProcessorConfig, output) 
{

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