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

    Frame(Node * ownerNode);
    ~Frame();
    void Add(Column * col); // Add column to frame
    void Free(); // Returm this frame to the owners pool
};

} // namespace lambda

#endif /* LAMBDA_FRAME_HPP */