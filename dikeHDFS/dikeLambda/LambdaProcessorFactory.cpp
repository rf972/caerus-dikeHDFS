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
    verbose = std::stoi(dikeProcessorConfig["system.verbose"]);

    if(verbose) {
        std::cout << dikeProcessorConfig["Name"] << " " << dikeProcessorConfig["ID"] << std::endl;
    }

    if (dikeProcessorConfig["Name"].compare("Lambda") == 0) { // Lambda request
        if(processorMap.count(dikeProcessorConfig["ID"]) > 0) { // We have Read Ahead Processor running
            processorMap[dikeProcessorConfig["ID"]]->Run(dikeProcessorConfig, output);
        } else { // Normal handling
            LambdaProcessor lambdaProcessor;
            lambdaProcessor.Init(dikeProcessorConfig, output);
            lambdaProcessor.Run(dikeProcessorConfig, output);
        }
    }  else  if (dikeProcessorConfig["Name"].compare("LambdaReadAhead") == 0) { // Lambda Read Ahead request
        // Allocate Read Ahead Processor
        LambdaProcessorReadAhead * lambdaProcessorReadAhead = new LambdaProcessorReadAhead;
        lambdaProcessorReadAhead->Init(dikeProcessorConfig, output);
        processorMap[dikeProcessorConfig["ID"]] = lambdaProcessorReadAhead;        
    } 
    return 0;
}

std::map<std::string, LambdaProcessorReadAhead *> LambdaProcessorFactory::processorMap;
