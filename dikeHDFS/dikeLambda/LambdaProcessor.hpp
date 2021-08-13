#ifndef LAMBDA_PROCESSOR
#define LAMBDA_PROCESSOR

#include "DikeProcessor.hpp"

class LambdaProcessor : public DikeProcessor {    
    public:
    int verbose = 0;
    LambdaProcessor(){};

    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual void Worker() override {};
};

#endif /* LAMBDA_PROCESSOR */