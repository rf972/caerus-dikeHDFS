#ifndef LAMBDA_FILTER_NODE_HPP
#define LAMBDA_FILTER_NODE_HPP

#include "LambdaNode.hpp"

namespace lambda {

class Filter;

class FilterNode : public Node {
    public:

    uint8_t * result = NULL;
    std::vector<Filter *> filterArray;
    FilterNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    ~FilterNode();

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
};

} // namespace lambda

#endif /* LAMBDA_FILTER_NODE_HPP */