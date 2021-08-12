#ifndef LAMBDA_NODE_HPP
#define LAMBDA_NODE_HPP

#include <string>
#include <queue>

#include "Poco/JSON/Object.h"

#include <parquet/arrow/reader.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>

#include "DikeUtil.hpp"
#include "LambdaProcessor.hpp"
#include "LambdaFrame.hpp"

namespace lambda {

class Node;

class Node {
    public:
    enum NodeType {
        _INVALID = 0,
        _INPUT = 0,
        _PROJECTION = 0,
        _OUTPUT = 0,
    };
    std::string name;
    Node::NodeType type;

    Node * nextNode = NULL; // One connection for now
    std::queue<Frame *> frameQueue; // Used as incoming queue
    std::queue<Frame *> framePool; // Used as memory pool for frames

    bool done = false;
    int verbose = 0;

    Node(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
        name = pObject->getValue<std::string>("Name");
        std::string typeStr = pObject->getValue<std::string>("Type");
        std::cout << "Node " << name << " type " << typeStr << std::endl;
        verbose = std::stoi(dikeProcessorConfig["system.verbose"]);
    }

    void Connect(Node * node){
        this->nextNode = node;
    }

    virtual void Init() { }

    virtual void UpdateColumnMap(Frame * frame) {
        std::cout << "UpdateColumnMap " << name  << std::endl;
        if(nextNode != NULL){
            nextNode->UpdateColumnMap(frame);
        }
    }

    virtual void putFrame(Frame * frame) { // Submit frame for processing
        frameQueue.push(frame);
    }

    virtual Frame * getFrame() { // Retrieve frame from incoming queue
        Frame * frame = frameQueue.front();
        frameQueue.pop();
        return frame;
    }

    Frame * allocFrame() {
        Frame * frame = framePool.front();
        framePool.pop();
        return frame;           
    }

    void freeFrame(Frame * frame) {
        framePool.push(frame);
    }

    virtual void Step() = 0;
};

class InputNode : public Node {
    public:
    std::shared_ptr<arrow::io::HadoopFileSystem> fs;
    static std::map< int, std::shared_ptr<arrow::io::HadoopFileSystem> > hadoopFileSystemMap;
    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
    std::shared_ptr<parquet::FileMetaData> fileMetaData;
    static std::map< std::string, std::shared_ptr<parquet::FileMetaData> > fileMetaDataMap;
    const parquet::SchemaDescriptor* schemaDescriptor;
    static std::map< std::string, std::string > fileLastAccessTimeMap;
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader;
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader;    

    int rowCount = 0; // How many rows we processed
    int numRows = 0;  // Total number of rows 
    int columnCount = 0;    
    std::shared_ptr<parquet::ColumnReader> * columnReaders;
    Column::DataType * columnTypes;

    InputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);
    ~InputNode();

    virtual void Init() override;
    virtual void Step() override;
};

class ProjectionNode : public Node {
    public:
    std::vector<std::string> projection;
    int columnCount = 0;
    std::vector<int> columnMap;

    ProjectionNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
    : Node(pObject, dikeProcessorConfig, output)  
    {
        std::cout << "ProjectionNode::ProjectionNode ";
        Poco::JSON::Array::Ptr projectionArray = pObject->getArray("ProjectionArray");
        columnCount =  projectionArray->size();
        columnMap.resize(columnCount);
        for(int i = 0; i < columnCount; i++){
            std::string name = projectionArray->get(i);
            projection.push_back(name);
            std::cout << name <<  " " ;
        }
        std::cout << std::endl;
    }

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual void Step() override;
};

class OutputNode : public Node {
    public:
    DikeIO * output = NULL;
    OutputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
        : Node(pObject, dikeProcessorConfig, output) 
    {
        std::cout << "OutputNode::OutputNode" << std::endl;
        this->output = output;
    }

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual void Step() override;
};

Node * CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);

} // namespace lambda

#endif /* LAMBDA_NODE_HPP */