#ifndef LAMBDA_FRAME_HPP
#define LAMBDA_FRAME_HPP

#include <vector>

#include "LambdaColumn.hpp"

namespace lambda {

class Node;
class Frame;

class Frame {
    public:
    static std::atomic<int> allocCount;
    static std::atomic<int> freeCount;

    std::vector<Column *> columns;
    Node * ownerNode = NULL;

    int refCount;
    Frame * parentFrame = NULL;
    bool lastFrame = false;
    
    int totalRowGroups = 0;

    Frame(Node * ownerNode);
    ~Frame();
    void Add(Column * col); // Add column to frame
    void Free(); // Returm this frame to the owners pool
    void ApplyFilter(uint8_t * filter);
};

} // namespace lambda

#endif /* LAMBDA_FRAME_HPP */