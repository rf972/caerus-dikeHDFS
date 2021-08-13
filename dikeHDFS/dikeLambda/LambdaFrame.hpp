#ifndef LAMBDA_FRAME_HPP
#define LAMBDA_FRAME_HPP

#include <vector>

#include "LambdaColumn.hpp"

namespace lambda {

class Node;
class Frame;

class Frame {
    public:
    std::vector<Column *> columns;
    Node * ownerNode = NULL;

    int refCount;
    Frame * parentFrame = NULL;
    bool lastFrame = false;

    Frame(Node * ownerNode) {
        this->ownerNode = ownerNode;
    }
    ~Frame(){
        for(int i = 0; i < columns.size(); i++) {
            columns[i]->refCount--;
            if(columns[i]->refCount == 0) {
                delete columns[i];
            }
        }
    }
    void Add(Column * col) {
        columns.push_back(col);
    }

    void Free(); // Returm this frame to the owners pool
};

} // namespace lambda

#endif /* LAMBDA_FRAME_HPP */