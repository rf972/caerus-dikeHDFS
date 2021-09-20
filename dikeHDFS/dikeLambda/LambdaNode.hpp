#ifndef LAMBDA_NODE_HPP
#define LAMBDA_NODE_HPP

#include <string>
#include <queue>
#include <mutex>
#include <semaphore.h>
#include <vector>

#include "Poco/JSON/Object.h"

#include <parquet/arrow/reader.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>

#include <zstd.h>   

#include "LambdaFileReader.hpp"
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
    std::mutex frameQueueMutex;
    sem_t frameQueueSem;

    std::queue<Frame *> framePool; // Used as memory pool for frames
    std::mutex framePoolMutex;
    sem_t framePoolSem;

    bool done = false;
    int verbose = 0;

    // Statistics
    int stepCount = 0;
    std::chrono::duration<double, std::milli> runTime = std::chrono::milliseconds(0);
    uint64_t recordsIn = 0;
    uint64_t recordsOut = 0;
    
    Node(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
        name = pObject->getValue<std::string>("Name");
        std::string typeStr = pObject->getValue<std::string>("Type");        
        verbose = std::stoi(dikeProcessorConfig["system.verbose"]);
        if(verbose){
            std::cout << "Node " << name << " type " << typeStr << std::endl;
        }
        sem_init(&frameQueueSem, 0, 0);
        sem_init(&framePoolSem, 0, 0);
    }

    virtual ~Node() {
        //std::cout << "Node::~Node " << name << std::endl;
        while(!framePool.empty()) {
            delete framePool.front();
            framePool.pop();
        }
        sem_destroy(&frameQueueSem);
        sem_destroy(&framePoolSem);        
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
        frameQueueMutex.lock();        
        frameQueue.push(frame);
        frameQueueMutex.unlock();
        sem_post(&frameQueueSem);
    }

    virtual Frame * getFrame() { // Retrieve frame from incoming queue
        sem_wait(&frameQueueSem);        
        frameQueueMutex.lock();
        if(frameQueue.empty()){
            frameQueueMutex.unlock();
            return NULL;
        }
        Frame * frame = frameQueue.front();
        frameQueue.pop();
        frameQueueMutex.unlock();
        return frame;
    }

    Frame * allocFrame() {
        sem_wait(&framePoolSem);
        framePoolMutex.lock();
        if(framePool.empty()){
            framePoolMutex.unlock();
            return NULL;
        }
        Frame * frame = framePool.front();
        framePool.pop();
        framePoolMutex.unlock();
        //std::cout << "Pop frame  " << frame <<  " Node " << name << std::endl;
        return frame;           
    }

    void freeFrame(Frame * frame) {
        //std::cout << "Push frame  " << frame <<  " Node " << name << std::endl;
        framePoolMutex.lock();
        framePool.push(frame);
        framePoolMutex.unlock();
        sem_post(&framePoolSem);
    }

    virtual bool Step() = 0;
    virtual void Worker() {
        bool done = false;
        while(!done) {
            done = Step();
        }
    }
    std::thread startWorker() {
        //std::cout << "DikeAsyncWriter::startWorker " << std::endl;
        return std::thread([this] { this->Worker(); });
    }    
};

class InputNode : public Node {
    public:
    std::shared_ptr<arrow::io::HadoopFileSystem> fs;
    static std::map< int, std::shared_ptr<arrow::io::HadoopFileSystem> > hadoopFileSystemMap;
#ifdef LEGACY_HDFS    
    std::shared_ptr<arrow::io::HdfsReadableFile> inputFile;
#else
    std::shared_ptr<ReadableFile> inputFile;
    static std::map< std::string, std::shared_ptr<ReadableFile> > inputFileMap;
#endif

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
    uint32_t compressedBufferLen = Column::MAX_TEXT_SIZE + 1024;
    uint8_t lz4_state_memory [32<<10] __attribute__((aligned(128))); // see (int LZ4_sizeofState(void);)
    std::vector<ZSTD_CCtx *> ZSTD_Context;
    int compressionLevel = 3;
    int dikeNodeType = 0;

    OutputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
        : Node(pObject, dikeProcessorConfig, output) 
    {        
        this->output = output;

        if(dikeProcessorConfig.count("dike.node.type") > 0) {            
            dikeNodeType = std::stoi(dikeProcessorConfig["dike.node.type"]);
            if(verbose){
                std::cout << "dikeNodeType " << dikeNodeType << std::endl;
            }           
        }

        if(pObject->has("CompressionType") && dikeNodeType == 1){ // Storage Node
            std::string compressionType = pObject->getValue<std::string>("CompressionType");
            if(verbose){
                std::cout << "CompressionType " << compressionType << std::endl;
            }
            if(compressionType.compare("ZSTD") == 0){
                compressionEnabled = true;
                compressionLevel = pObject->getValue<int>("CompressionLevel");
                compressedBufferLen = ZSTD_compressBound(Column::MAX_TEXT_SIZE);
                compressedBuffer = new uint8_t [compressedBufferLen]; // Max text lenght + compression header                
            }
        }

        lenBuffer = new uint8_t [Column::MAX_SIZE];
        dataBuffer = new uint8_t [Column::MAX_TEXT_SIZE]; // Max text lenght        
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
        for (int i = 0; i < ZSTD_Context.size(); i++) {
            ZSTD_freeCCtx(ZSTD_Context[i]);
        }
    }

    virtual void UpdateColumnMap(Frame * frame) override;
    virtual bool Step() override;
    void CompressZlib(uint8_t * data, uint32_t len, bool is_binary);
    void CompressLZ4(uint8_t * data, uint32_t len);
    void CompressZSTD(int id, uint8_t * data, uint32_t len);

    void TranslateBE64(void * in_data, uint8_t * out_data, uint32_t len);
    void Send(void * data, uint32_t len, bool is_binary);
    // New format
    void Send(int id, Column::DataType data_type, int type_size, void * data, uint32_t len);
};

Node * CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output);

} // namespace lambda

#endif /* LAMBDA_NODE_HPP */