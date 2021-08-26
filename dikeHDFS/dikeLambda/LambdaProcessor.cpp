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

int LambdaProcessor::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    std::vector<Node *> nodeVector;
    verbose = std::stoi(dikeProcessorConfig["system.verbose"]);

    if (verbose) {
        std::cout << "LambdaProcessor::Run" << std::endl;
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

    // Initialize Nodes
    for(int i = 0; i < nodeVector.size(); i++) {
        nodeVector[i]->Init();
    }

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

    // Start output worker
    Node * outputNode = nodeVector[nodeVector.size() - 1];    
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
        std::cout << "Records " << record_counter;
        std::cout << " run_time " << run_time.count()/ 1000 << " sec" << std::endl;
        
    }

    for(int i = 0; i < nodeVector.size(); i++){
        //std::cout << "Deleting Node " << nodeVector[i]->name << std::endl;
        delete nodeVector[i];
    }

    return(0);
}
