
#include "LambdaFrame.hpp"
#include "LambdaNode.hpp"

using namespace lambda;

Frame::Frame(Node * ownerNode) {
    this->ownerNode = ownerNode;
    //std::cout << "New frame  " << this <<  " Node " << ownerNode->name << std::endl;
}

Frame::~Frame(){
    //std::cout << "Delete frame  " << this <<  " Node " << ownerNode->name << std::endl;
    
    for(int i = 0; i < columns.size(); i++) {        
        if(columns[i]->ownerNode == ownerNode) {
            delete columns[i];
        }
    }    
}

void Frame::Add(Column * col) {
    columns.push_back(col);
}

void Frame::Free()
{
    //std::cout << "Free frame  " << this <<  " Node " << ownerNode->name << std::endl;
    if(parentFrame){
        parentFrame->Free();
        parentFrame = 0;
    }
    ownerNode->freeFrame(this);
}

void Frame::ApplyFilter(uint8_t * filter)
{
    for(int i = 0; i < columns.size(); i++) {        
        columns[i]->ApplyFilter(filter);
    }
}