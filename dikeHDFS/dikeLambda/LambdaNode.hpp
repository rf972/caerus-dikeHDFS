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

    // Statistics
    int stepCount = 0;

    Node(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
        name = pObject->getValue<std::string>("Name");
        std::string typeStr = pObject->getValue<std::string>("Type");        
        verbose = std::stoi(dikeProcessorConfig["system.verbose"]);
        if(verbose){
            std::cout << "Node " << name << " type " << typeStr << std::endl;
        }
    }

    virtual ~Node() {
        while(!framePool.empty()) {
            delete framePool.front();
            framePool.pop();
        }
    }
    void Connect(Node * node){
        this->nextNode = node;
    }

    virtual void Init() { }

    virtual void UpdateColumnMap(Frame * frame) {
        //std::cout << "UpdateColumnMap " << name  << std::endl;
        if(nextNode != NULL){
            nextNode->UpdateColumnMap(frame);
        }
    }

    virtual void putFrame(Frame * frame) { // Submit frame for processing
        frameQueue.push(frame);
    }

    virtual Frame * getFrame() { // Retrieve frame from incoming queue
        if(frameQueue.empty()){
            return NULL;
        }
        Frame * frame = frameQueue.front();
        frameQueue.pop();
        return frame;
    }

    Frame * allocFrame() {
        if(framePool.empty()){
            return NULL;
        }
        Frame * frame = framePool.front();
        framePool.pop();
        return frame;           
    }

    void freeFrame(Frame * frame) {
        framePool.push(frame);
    }

    virtual bool Step() = 0;
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
    virtual ~InputNode();

    virtual void Init() override;
    virtual bool Step() override;
};

class ProjectionNode : public Node {
    public:
    std::vector<std::string> projection;
    int columnCount = 0;
    std::vector<int> columnMap;

    ProjectionNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
    : Node(pObject, dikeProcessorConfig, output)  
    {
        Poco::JSON::Array::Ptr projectionArray = pObject->getArray("ProjectionArray");
        columnCount =  projectionArray->size();
        columnMap.resize(columnCount);
        for(int i = 0; i < columnCount; i++){
            std::string name = projectionArray->get(i);
            projection.push_back(name);
        }
    }

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
};

class OutputNode : public Node {
    public:
    bool compressionEnabled = false;

    DikeIO * output = NULL;
    uint8_t * lenBuffer = NULL;
    uint8_t * dataBuffer = NULL;

    uint8_t * compressedBuffer = NULL;
    int64_t compressedLen = 0;
    uint32_t compressedBufferLen = Column::MAX_SIZE * 128 + 1024;

    OutputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
        : Node(pObject, dikeProcessorConfig, output) 
    {        
        this->output = output;

        if(pObject->has("CompressionType")){
            std::string compressionType = pObject->getValue<std::string>("CompressionType");
            if(verbose){
                std::cout << "CompressionType " << compressionType << std::endl;
            }
            if(compressionType.compare("zlib") == 0){
                compressionEnabled = true;
                compressedBuffer = new uint8_t [compressedBufferLen]; // Max text lenght + compression header
            }
        }

        lenBuffer = new uint8_t [Column::MAX_SIZE];
        dataBuffer = new uint8_t [Column::MAX_SIZE * 128]; // Max text lenght
        
    }

    virtual ~OutputNode(){
        if(lenBuffer){
            delete [] lenBuffer;
        }
        if(dataBuffer){
            delete [] dataBuffer;
        }
        if(compressedBuffer){
            delete [] compressedBuffer;
        }

    }
    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
    void CompressZlib(uint8_t * data, uint32_t len, bool is_binary);
    void CompressLZ4(uint8_t * data, uint32_t len);

    void TranslateBE64(void * in_data, uint8_t * out_data, uint32_t len);
    void Send(void * data, uint32_t len, bool is_binary);
};

Node * CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);

} // namespace lambda

#endif /* LAMBDA_NODE_HPP */