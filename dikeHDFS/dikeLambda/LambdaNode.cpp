#include <Poco/URI.h>
#include "Poco/Thread.h"
#include <Poco/Net/HTTPRequest.h>

#include "LambdaNode.hpp"


using namespace lambda;

InputNode::InputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
: Node(pObject, dikeProcessorConfig, output) 
{
    std::cout << "InputNode::InputNode" << std::endl;
    arrow::io::HdfsConnectionConfig hdfsConnectionConfig;
    std::stringstream ss;
    ss.str(dikeProcessorConfig["Request"]);
    Poco::Net::HTTPRequest hdfs_req;
    hdfs_req.read(ss);

    Poco::URI uri = Poco::URI(hdfs_req.getURI());
    Poco::URI::QueryParameters uriParams = uri.getQueryParameters();        
    for(int i = 0; i < uriParams.size(); i++){
        if(uriParams[i].first.compare("namenoderpcaddress") == 0){
            std::string rpcAddress = uriParams[i].second;
            auto pos = rpcAddress.find(':');
            hdfsConnectionConfig.host = rpcAddress.substr(0, pos);
            hdfsConnectionConfig.port = std::stoi(rpcAddress.substr(pos + 1, rpcAddress.length()));
        } else if(uriParams[i].first.compare("user.name") == 0) {
            hdfsConnectionConfig.user = uriParams[i].second;                
        }            
    }
    std::string path = uri.getPath();
    std::string fileName = path.substr(11, path.length()); // skip "/webhdfs/v1"
    int rowGroupIndex = std::stoi(dikeProcessorConfig["Configuration.RowGroupIndex"]);

    Poco::Thread * current = Poco::Thread::current();
    int threadId = current->id();        
            
    if (hadoopFileSystemMap.count(threadId)) {
        fs = hadoopFileSystemMap[threadId];
        //std::cout << " LambdaParquetReader id " << threadId << " reuse FS connection "<< std::endl;
    } else {
        arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);
        hadoopFileSystemMap[threadId] = fs;
        //std::cout << " LambdaParquetReader id " << threadId << " create FS connection "<< std::endl;
    }
                                    
    fs->OpenReadable(fileName, &inputFile);                        

    if (fileMetaDataMap.count(fileName)) {
        if (0 == fileLastAccessTimeMap[fileName].compare(dikeProcessorConfig["Configuration.LastAccessTime"])){
            fileMetaData = fileMetaDataMap[fileName];                
        } else {
            fileMetaData = std::move(parquet::ReadMetaData(inputFile));
            fileMetaDataMap[fileName] = fileMetaData;
            fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];                
        }            
    } else {
        fileMetaData = std::move(parquet::ReadMetaData(inputFile));
        fileMetaDataMap[fileName] = fileMetaData;
        fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];            
    }
    
    schemaDescriptor = fileMetaData->schema();
    parquetFileReader = std::move(parquet::ParquetFileReader::Open(inputFile, parquet::default_reader_properties(), fileMetaData));
    rowGroupReader = std::move(parquetFileReader->RowGroup(rowGroupIndex));

    numRows = rowGroupReader->metadata()->num_rows();
    columnCount = schemaDescriptor->num_columns();
    
    columnReaders = new std::shared_ptr<parquet::ColumnReader> [columnCount];
    columnTypes = new Column::DataType[columnCount];    
}

void InputNode::Init() 
{    
    Frame * frame = new Frame(this);
    for(int i = 0; i < columnCount; i++){        
        columnReaders[i] = NULL;
        auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
        std::string name = columnRoot->name();
        parquet::Type::type physical_type = columnRoot->physical_type();
        columnTypes[i] = (Column::DataType) physical_type; // TODO type tramslation
        Column * col = new Column(i, name,  columnTypes[i]); 
        frame->Add(col);
    }    

    // This will send update request down to graph
    UpdateColumnMap(frame);

    for(int i = 0; i < columnCount; i++){
        if(frame->columns[i]->refCount > 0) {
            std::cout << "Create reader for Column " << i << std::endl;
            columnReaders[i] = rowGroupReader->Column(i);
        }
    }
    delete frame;
}

void InputNode::Step()
{
    if(done) { return; }

    int size = std::min((int)Column::MAX_SIZE, numRows - rowCount);
    Frame * frame = NULL;

    frame = allocFrame();
    if(frame == NULL){
        frame = new Frame(this); // Allocate new frame with data    
        for(int i = 0; i < columnCount; i++){        
            if(columnReaders[i]) {
                auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
                std::string name = columnRoot->name();            
                Column * col = new Column(i, name,  columnTypes[i]);
                col->Init();                
                frame->Add(col);
            }
        }
    }

    for(int i = 0; i < columnCount; i++){
        if(columnReaders[i]) {
            frame->columns[i]->Read(columnReaders[i], size);
        }
    }
    rowCount += size;
    nextNode->putFrame(frame); // Send frame down to graph
}

InputNode::~InputNode() 
{
    if(columnReaders) {
        for(int i = 0; i < columnCount; i++) {
            if(columnReaders[i]) {
                columnReaders[i].reset(); // This should dereference shared_ptr
            }
        }

        delete [] columnReaders;
    }
    if(columnTypes){
        delete [] columnTypes;
    }

    parquetFileReader->Close();
    inputFile->Close();    
}

void ProjectionNode::UpdateColumnMap(Frame * inFrame) 
{
    for(int i = 0; i < columnCount; i++){
        for(int j = 0; j < inFrame->columns.size(); j++){
            if(projection[i].compare(inFrame->columns[j]->name) == 0){
                inFrame->columns[j]->refCount ++; // This column will be in use
                columnMap[i] = j;
            }
        }
    }
    Frame * outFrame = new Frame(this);
    outFrame->columns.resize(columnCount);
    for(int i = 0; i < columnCount; i++) {
        outFrame->columns[i] = inFrame->columns[columnMap[i]]->Clone(); // TODO ref counting
    }

    Node::UpdateColumnMap(outFrame);
    delete outFrame;
}

void ProjectionNode::Step()
{
    Frame * inFrame = getFrame();    
    if(inFrame == NULL) return;

    Frame * outFrame = allocFrame();
    if(outFrame == NULL){
        outFrame = new Frame(this); // Allocate new frame with data
        outFrame->columns.resize(columnCount);
    }

    for(int i = 0; i < columnCount; i++) {
        outFrame->columns[i] = inFrame->columns[columnMap[i]]; // TODO ref counting
    }
    nextNode->putFrame(outFrame); // Send frame down to graph
}

void OutputNode::UpdateColumnMap(Frame * frame) 
{
    // This is our first write, so buffer should have enough space
    int64_t be_value = htobe64(frame->columns.size());
    output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
    for( int i  = 0; i < frame->columns.size(); i++){
        be_value = htobe64(frame->columns[i]->data_type);
        output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
    }
}

void OutputNode::Step()
{
    Frame * inFrame = getFrame();
    if(inFrame == NULL) return;

    Column * col = 0;

    for( int i  = 0; i < inFrame->columns.size(); i++){

    }
}

void Frame::Free()
{
    if(parentFrame){
        parentFrame->Free();
        parentFrame = 0;
    }
    ownerNode->freeFrame(this);
}


Node * lambda::CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
    std::string typeStr = pObject->getValue<std::string>("Type");
    if(typeStr.compare("_INPUT") == 0){
        return new InputNode(pObject, dikeProcessorConfig, output);
    }
    if(typeStr.compare("_PROJECTION") == 0){
        return new ProjectionNode(pObject, dikeProcessorConfig, output);
    }
    if(typeStr.compare("_OUTPUT") == 0){
        return new OutputNode(pObject, dikeProcessorConfig, output);
    }
    std::cout << "Uknown Node Type : " << typeStr << std::endl;
    return NULL;
}

std::map<int, std::shared_ptr<arrow::io::HadoopFileSystem> > lambda::InputNode::hadoopFileSystemMap;
std::map< std::string, std::shared_ptr<parquet::FileMetaData> >  lambda::InputNode::fileMetaDataMap;
std::map< std::string, std::string > lambda::InputNode::fileLastAccessTimeMap;
