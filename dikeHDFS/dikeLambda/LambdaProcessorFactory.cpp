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

#include <malloc.h> // for malloc_trim

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
            int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);
            lambdaProcessor.Run(rowGroupIndex, output);
            //lambdaProcessor.Run(rowGroupIndex+1, output);
            lambdaProcessor.Finish();
        }
    }  else  if (dikeProcessorConfig["Name"].compare("LambdaReadAhead") == 0) { // Lambda Read Ahead request
        // Allocate Read Ahead Processor
        LambdaProcessorReadAhead * lambdaProcessorReadAhead = new LambdaProcessorReadAhead;
        lambdaProcessorReadAhead->Init(dikeProcessorConfig, output);
        processorMap[dikeProcessorConfig["ID"]] = lambdaProcessorReadAhead;        
    } else  if (dikeProcessorConfig["Name"].compare("LambdaTotal") == 0) { // Lambda Whole file processor
        // Allocate Total
        LambdaProcessorTotal * lambdaProcessorTotal = new LambdaProcessorTotal;
        lambdaProcessorTotal->Init(dikeProcessorConfig, output);
        processorMap[dikeProcessorConfig["ID"]] = lambdaProcessorTotal;
    } else   if (dikeProcessorConfig["Name"].compare("LambdaInfo") == 0) { // Lambda Info request
        std::string resp;
        if(processorMap.count(dikeProcessorConfig["ID"]) > 0) {
            LambdaProcessor * lambdaProcessor = processorMap[dikeProcessorConfig["ID"]];            
            resp = "PartitionCount = " + std::to_string(lambdaProcessor->totalResults) + "\n";
        } else {
            resp = "Uknown ID " + dikeProcessorConfig["ID"];
        }
        output->write(resp.c_str(), resp.length());
    } else  if (dikeProcessorConfig["Name"].compare("LambdaClearAll") == 0) { // Lambda Clear All 
        for (auto it : processorMap) {
            //std::cout << "Deleting " << it.first << std::endl;
            delete it.second;
        }
        malloc_trim(0);
        processorMap.clear();
        std::string resp("All clear");
        output->write(resp.c_str(), resp.length());
    }
    return 0;
}

std::map<std::string, LambdaProcessor *> LambdaProcessorFactory::processorMap;
