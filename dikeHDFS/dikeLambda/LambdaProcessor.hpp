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

class LambdaProcessorReadAhead : public LambdaProcessor {    
    public:
    DikeProcessorConfig dikeProcessorConfig;
    DikeIO * output;
    LambdaProcessorReadAhead(){};

    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual void Worker() override;
};

class LambdaProcessorFactory : public LambdaProcessor {    
    public:
    DikeProcessorConfig dikeProcessorConfig;
    DikeIO * output;
    LambdaProcessorFactory(){};

    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual void Worker() override {};
};


#endif /* LAMBDA_PROCESSOR */