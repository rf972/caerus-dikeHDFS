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

void LambdaProcessorTotal::CreateGraphs(DikeProcessorConfig & dikeProcessorConfig)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(dikeProcessorConfig["Configuration.DAG"]);
    Poco::JSON::Object::Ptr pObject = result.extract<Poco::JSON::Object::Ptr>();
    Poco::JSON::Array::Ptr nodeArray = pObject->getArray("NodeArray");

    nodeTree.resize(nWorkers);
    // Create nodes
    for(int n = 0; n < nWorkers; n++){
        for(int i = 0; i < nodeArray->size(); i++){
            Poco::JSON::Object::Ptr pNodeObject = nodeArray->getObject(i);

            if(pNodeObject->has("Barrier")) {
                int barrier = std::stoi(pNodeObject->getValue<std::string>("Barrier"));
                if(barrier > 0 && n > 0) { // Only one trunk after barrier
                    break;
                }
            }
            Node * node = CreateNode(pNodeObject, dikeProcessorConfig, NULL);
            nodeTree[n].push_back(node);
        }
    }

    // Connect Nodes
    for(int n = 0; n < nWorkers; n++){
        int i;
        for(i = 0; i < nodeTree[n].size() - 1; i++) {
            std::cout << "Connect " << nodeTree[n][i]->name << " to " << nodeTree[n][i+1]->name << std::endl;
            nodeTree[n][i]->Connect(nodeTree[n][i+1]);            
        }
        if( n > 0) { // Connect last node to barrier
            std::cout << "Connect " << nodeTree[n][i]->name << " to " << nodeTree[0][i+1]->name << std::endl;
            nodeTree[n][i]->Connect(nodeTree[0][i+1]);
        }
    }

    // UpdateColumnMap
    for(int n = 0; n < nWorkers; n++){
        nodeTree[n][0]->UpdateColumnMap(NULL);
    }

}

void LambdaProcessorTotal::Init(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{
    std::string resp("LambdaProcessorTotal::Init [" + dikeProcessorConfig["ID"] + "]");
    //std::cout << resp << std::endl;
    output->write(resp.c_str(), resp.length());

    nWorkers = std::stoi(dikeProcessorConfig["dike.storage.processor.workers"]);
    workerThread.resize(nWorkers + 1);
    
    // Create all graphs 
    CreateGraphs(dikeProcessorConfig);

    // Get rowGroupCount from INPUT node
    rowGroupCount = ((InputNode *)nodeTree[0][0])->rowGroupCount;
    lambdaResultVector = new LambdaResultVector(rowGroupCount, nWorkers*2);

    // This will be reported to Spark as single rowGroup - single partition
    totalResults = 1;
        
    for(int i = 0; i < nWorkers; i++) {        
        workerThread[i] = std::thread([=] { Worker(i); });
    }
    
    workerThread[nWorkers] = std::thread([=] { TrunkWorker(); });
}

LambdaProcessorTotal::~LambdaProcessorTotal() {
    std::cout << "~LambdaProcessorTotal()" << std::endl;
    done = true;
    // Do it for each worker
    for(int i = 0; i < workerThread.size(); i++) {
        sem_post(&lambdaResultVector->sem);
    }

    for(int i = 0; i < workerThread.size(); i++) {
        workerThread[i].join();
    }

    if(lambdaResultVector){
        delete lambdaResultVector;
    }

    for(int n = 0; n < nWorkers; n++){        
        for(int i = nodeTree[n].size() - 1; i >= 0; i--) {
            delete nodeTree[n][i];            
        }
    }    
}

// This will simply send back results
int LambdaProcessorTotal::Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output)
{    
    int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);

    // TODO make sure that rowGroupIndex == 0
    
    //std::string resp("LambdaProcessorTotal::Run [" + dikeProcessorConfig["ID"] + "]");
    //std::cout << resp << " " << rowGroupIndex << std::endl;    
    
    LambdaResult * res = lambdaResultVector->resultVector[0];
    sem_wait(&res->sem); // This will change when we will support NCP
    lambdaResultVector->lock.lock();
    if (res->state == LambdaResult::READY) {
        res->state = LambdaResult::DONE;
        lambdaResultVector->lock.unlock();
        //std::cout << "LambdaProcessorTotal::Run Sending results for " << rowGroupIndex << std::endl;
        // Send results to output        
        for(int i = 0; i < res->buffers.size(); i++){
            output->write((const char * )res->buffers[i]->ptr, res->buffers[i]->len);
        }
        //std::cout << "LambdaProcessorTotal::Run Sending for " << rowGroupIndex << " completed " << std::endl;
        lambdaBufferPool.Free(res->buffers);
    } else {
        lambdaResultVector->lock.unlock();
    }

    //std::cout << "Wake up worker " << std::endl;
    sem_post(&lambdaResultVector->sem);

    return 0;
}

void LambdaProcessorTotal::Worker(int workerID)
{       
    int result_index;

    std::vector<Node *> branch;
    for(int i = 0; i <  nodeTree[workerID].size(); i++) {
        if(nodeTree[workerID][i]->barrier) {
            break;
        }
        branch.push_back(nodeTree[workerID][i]);
    }                
    
    do {
        // wait on lambdaResultVector->sem
        sem_wait(&lambdaResultVector->sem);        
        if(done) {
            continue;
        }
        // Lock lambdaResultVector
        lambdaResultVector->lock.lock();
        // Find next index to operate
        result_index = -1;
        for(int i = 0; i < rowGroupCount && result_index == -1; i++){
            LambdaResult * res = lambdaResultVector->resultVector[i];
            if (res->state == LambdaResult::EMPTY){
                res->state = LambdaResult::PENDING;
                result_index = i;
                std::cout << "LambdaProcessorTotal::Worker " << workerID << " Run on " << result_index << std::endl;
            }
        }
        lambdaResultVector->lock.unlock();
        if(result_index >= 0){            
            // Initialize Nodes
            for(int i = 0; i <  branch.size(); i++) {
                branch[i]->Init(result_index);
            }            

            // Run graph
            bool step_done = false;
            while(!step_done) {
                for(int i = 0; i <  branch.size(); i++) {
                   step_done = branch[i]->Step();
                }        
            }

            //lambdaResultVector->lock.lock();
            //lambdaResultVector->resultVector[result_index]->state = LambdaResult::READY;
            //lambdaResultVector->lock.unlock();
        }
    } while(result_index >= 0 && ! done); // Repeat
    std::cout << "LambdaProcessorTotal::Worker " << workerID << " Done " << std::endl;
}

void LambdaProcessorTotal::TrunkWorker() 
{
    LambdaResultOutput output = LambdaResultOutput(lambdaResultVector->resultVector[0]);

    // Initialize Nodes
    std::vector<Node *> trunk;
    bool is_trunk = false;
    for(int i = 0; i <  nodeTree[0].size(); i++) {
        if(nodeTree[0][i]->barrier) {
            is_trunk = true;
        }
        if(is_trunk) {
            trunk.push_back(nodeTree[0][i]);            
        }
    }

    OutputNode * outputNode = (OutputNode *)trunk[trunk.size() - 1];
    outputNode->output = (DikeIO *)&output;

    for(int i = 0; i < trunk.size(); i++) {
        trunk[i]->Init(0);
    }

    // Run barrier node to the completion
    bool step_done = false;
    while(!step_done) {        
        step_done = trunk[0]->Step();
        sem_post(&lambdaResultVector->sem);
    }

    std::cout << "LambdaProcessorTotal::TrunkWorker " << " Barrier Done " << std::endl;

    // Run the rest of the graph
    step_done = false;
    while(!step_done) {
        for(int i = 1; i < trunk.size(); i++) {      
            step_done = trunk[i]->Step();
        }        
    }    
    LambdaResult * res = lambdaResultVector->resultVector[0];
    lambdaResultVector->lock.lock();
    res->state = LambdaResult::READY;
    lambdaResultVector->lock.unlock();

    sem_post(&res->sem);

    std::cout << "LambdaProcessorTotal::TrunkWorker " << " Done " << std::endl;
}