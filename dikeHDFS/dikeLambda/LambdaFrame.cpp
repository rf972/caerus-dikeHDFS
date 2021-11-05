
#include "LambdaFrame.hpp"
#include "LambdaNode.hpp"

using namespace lambda;

Frame::Frame(Node * ownerNode) {
    this->ownerNode = ownerNode;
    //std::cout << "New frame  " << this <<  " Node " << ownerNode->name << std::endl;
    allocCount++;
}

Frame::~Frame(){
    //std::cout << "Delete frame  " << this <<  " Node " << ownerNode->name << std::endl;
    freeCount--;
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

std::atomic<int> Frame::allocCount(0);
std::atomic<int> Frame::freeCount(0);

std::atomic<int> Column::allocCount(0);
std::atomic<int> Column::freeCount(0);