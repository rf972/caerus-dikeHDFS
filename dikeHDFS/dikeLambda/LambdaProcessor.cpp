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

void LambdaProcessor::Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{    
    verbose = std::stoi(dikeProcessorConfig["system.verbose"]);

    if (verbose) {
        std::cout << "LambdaProcessor::Init" << std::endl;
        std::cout << dikeProcessorConfig["Configuration.DAG"] << std::endl;
    }
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(dikeProcessorConfig["Configuration.DAG"]);
    Poco::JSON::Object::Ptr pObject = result.extract<Poco::JSON::Object::Ptr>();
    if (verbose) {
        std::string dagName = pObject->getValue<std::string>("Name");
        std::cout << dagName << std::endl;
    }

    Poco::JSON::Array::Ptr nodeArray = pObject->getArray("NodeArray");
    if (verbose) {
        std::cout << "Creating pipe with " << nodeArray->size() << " nodes " << std::endl;
    }
    for(int i = 0; i < nodeArray->size(); i++){
        nodeVector.push_back(CreateNode(nodeArray->getObject(i), dikeProcessorConfig, output));
    }

    if (0 && verbose) {
        for(int i = 0; i < nodeVector.size(); i++) {
            std::cout << "nodeVector[" << i << "]->name " << nodeVector[i]->name << std::endl;
        }
    }

    // Connect Nodes
    for(int i = 0; i < nodeVector.size() - 1; i++) {
        nodeVector[i]->Connect(nodeVector[i+1]);
    }

    // Get rowGroupCount from INPUT node
    rowGroupCount = ((InputNode *)nodeVector[0])->rowGroupCount;
}

int LambdaProcessor::Run(int rowGroupIndex, DikeIO * output)
{    
    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();   

    OutputNode * outputNode = (OutputNode *)nodeVector[nodeVector.size() - 1];
    outputNode->output = output;

    // Initialize Nodes
    for(int i = 0; i < nodeVector.size(); i++) {
        nodeVector[i]->Init(rowGroupIndex);
    }

    // Start output worker        
    std::thread outputThread = outputNode->startWorker();

    bool done = false;
    while(!done)     
    {
        for(int i = 0; i < nodeVector.size() - 1 ; i++) {
            done = nodeVector[i]->Step();
        }        
    }

    outputThread.join();
 
    if (verbose) {
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> run_time = t2 - t1;        

        std::cout << "Records " << outputNode->recordsOut;
        std::cout << " run_time " << run_time.count()/ 1000 << " sec" << std::endl;        

        std::chrono::duration<double, std::milli> totalRunTime = std::chrono::milliseconds(0);
        for(int i = 0; i < nodeVector.size(); i++){
            std::cout << "Node " << nodeVector[i]->name << " runTime " << nodeVector[i]->runTime.count()/ 1000 <<std::endl;
            totalRunTime += nodeVector[i]->runTime;        
        }

        std::cout << "CPU totalRunTime " << totalRunTime.count()/ 1000 << " sec" << std::endl;
        std::cout << "Actual run_time " << run_time.count()/ 1000 << " sec" << std::endl;
    }

    return(0);
}

void LambdaProcessor::Finish()
{
    for(int i = nodeVector.size() - 1; i >= 0; i--){
        //std::cout << "Deleting Node " << nodeVector[i]->name << std::endl;
        delete nodeVector[i];
    }
}