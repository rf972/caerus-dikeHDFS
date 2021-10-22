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
    } else   if (dikeProcessorConfig["Name"].compare("LambdaInfo") == 0) { // Lambda Info request
        std::string resp;
        if(processorMap.count(dikeProcessorConfig["ID"]) > 0) {
            LambdaProcessorReadAhead * lambdaProcessorReadAhead = processorMap[dikeProcessorConfig["ID"]];            
            resp = "PartitionCount = " + std::to_string(lambdaProcessorReadAhead->rowGroupCount) + "\n";
        } else {
            resp = "Uknown ID " + dikeProcessorConfig["ID"];
        }
        output->write(resp.c_str(), resp.length());
    } else  if (dikeProcessorConfig["Name"].compare("LambdaClearAll") == 0) { // Lambda Clear All 
        for (std::pair<std::string, LambdaProcessorReadAhead *> it : processorMap) {
            //std::cout << "Deleting " << it.first << std::endl;
            delete it.second;
        }
        processorMap.clear();
        std::string resp("All clear");
        output->write(resp.c_str(), resp.length());
    }
    return 0;
}

std::map<std::string, LambdaProcessorReadAhead *> LambdaProcessorFactory::processorMap;
