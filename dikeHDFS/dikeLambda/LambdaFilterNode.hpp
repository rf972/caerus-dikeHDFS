#ifndef LAMBDA_FILTER_NODE_HPP
#define LAMBDA_FILTER_NODE_HPP

#include "LambdaNode.hpp"

namespace lambda {

class FilterNode : public Node {
    public:

    FilterNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);    

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
};

} // namespace lambda

#endif /* LAMBDA_FILTER_NODE_HPP */