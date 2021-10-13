#include <string>
#include <sstream>
#include <chrono>
#include <stdio.h>
//#include <pthread.h>
#include <algorithm>
#include <vector>
#include <thread>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include "DikeUtil.hpp"
#include "LambdaProcessor.hpp"
#include "LambdaFrame.hpp"
#include "LambdaNode.hpp"
#include "LambdaFilterNode.hpp"

using namespace lambda;

int LambdaProcessorFactory::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    this->dikeProcessorConfig = dikeProcessorConfig;
    this->output = output;

    LambdaProcessor lambdaProcessor;
    lambdaProcessor.Run(dikeProcessorConfig, output);

    //startWorker();
    return 0;
}

