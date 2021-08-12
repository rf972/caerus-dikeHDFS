#include <Poco/URI.h>
#include "Poco/Thread.h"
#include <Poco/Net/HTTPRequest.h>

#include "LambdaNode.hpp"


using namespace lambda;

InputNode::InputNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) 
: Node(pObject, dikeProcessorConfig, output) 
{    
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
            //std::cout << "Create reader for Column " << i << std::endl;
            columnReaders[i] = std::move(rowGroupReader->Column(i));            
        } else {
            columnReaders[i] = NULL;
        }
    }
    delete frame;
}

bool InputNode::Step()
{
    //std::cout << "InputNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    int size = std::min((int)Column::MAX_SIZE, numRows - rowCount);
    Frame * frame = NULL;

    frame = allocFrame();
    if(frame == NULL){
        frame = new Frame(this); // Allocate new frame with data    
        for(int i = 0; i < columnCount; i++){                        
            auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
            std::string name = columnRoot->name();            
            Column * col = new Column(i, name,  columnTypes[i]);
            //std::cout << "Create Column " << i <<  " " << name << std::endl;            
            frame->Add(col);
            if(columnReaders[i]) {
                col->Init(); // This will allocate memory buffers
            }
        }
    } else {
        //std::cout << "We got frame from pool" << std::endl;
    }

    for(int i = 0; i < columnCount; i++){
        if(columnReaders[i]) {
            //std::cout << "Read Column " << colId <<  " " << frame->columns[i]->name << std::endl;
            frame->columns[i]->Read(columnReaders[i], size); 
        }       
    }
    rowCount += size;
    if(rowCount >= numRows){
        frame->lastFrame = true;
        done = true;
    }
    nextNode->putFrame(frame); // Send frame down to graph
    return done;
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
        outFrame->columns[i] = inFrame->columns[columnMap[i]]->Clone(); 
    }

    Node::UpdateColumnMap(outFrame);
    delete outFrame;
}

bool ProjectionNode::Step()
{
    //std::cout << "ProjectionNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    Frame * inFrame = getFrame();    
    if(inFrame == NULL) {
        std::cout << "Input queue is empty " << std::endl;
        return done;
    }

    Frame * outFrame = allocFrame();
    if(outFrame == NULL){
        outFrame = new Frame(this); // Allocate new frame with data
        outFrame->columns.resize(columnCount);
    }

    outFrame->parentFrame = inFrame;

    for(int i = 0; i < columnCount; i++) {
        //std::cout << "Mapping Column " << i <<  " to " << columnMap[i] << std::endl;
        outFrame->columns[i] = inFrame->columns[columnMap[i]]->Clone(); 
    }

    if(inFrame->lastFrame){
        outFrame->lastFrame = true;
        done = true;
    }

    nextNode->putFrame(outFrame); // Send frame down to graph
    return done;
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

bool OutputNode::Step()
{
    //std::cout << "OutputNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    Frame * inFrame = getFrame();
    if(inFrame == NULL) return done;

    Column * col = 0;
    int64_t be_value;
    for( int i  = 0; i < inFrame->columns.size(); i++){
        col = inFrame->columns[i];
        //std::cout << "Writing out " << col->name << std::endl;
        int64_t data_size;
        switch (col->data_type) {
            case Column::DataType::INT64:
                data_size = col->row_count * sizeof(int64_t);
                //std::cout << "data_size " << data_size << std::endl;
                be_value = htobe64(data_size);
                output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
                // Translate data to Big Endian
                {
                    int64_t * int64_ptr = (int64_t *)dataBuffer;
                    for(int j = 0; j < col->row_count; j ++) {
                        int64_ptr[j] = htobe64(col->int64_values[j]);
                    }
                }                
                output->write((const char *)dataBuffer, (uint32_t)data_size);
                break;
            case Column::DataType::DOUBLE:
                data_size = col->row_count * sizeof(double);
                //std::cout << "data_size " << data_size << std::endl;
                be_value = htobe64(data_size);
                output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
                // Translate data to Big Endian
                {
                    int64_t * int64_ptr = (int64_t *)dataBuffer;
                    for(int j = 0; j < col->row_count; j ++) {
                        int64_ptr[j] = htobe64(*(int64_t *)&col->double_values[j]);
                    }
                }                               
                output->write((const char *)dataBuffer, (uint32_t)data_size);
            break;
            case Column::DataType::BYTE_ARRAY:
                data_size = col->row_count;
                //std::cout << "data_size " << data_size << std::endl;
                be_value = htobe64(data_size);
                output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
                uint8_t * dataPtr = dataBuffer;        
                for(int j = 0; j < col->row_count; j++){
                    uint8_t len = col->ba_values[j].len;
                    lenBuffer[j] = len;            
                    for(int k = 0 ; k < len; k++){
                        *dataPtr = col->ba_values[j].ptr[k];
                        dataPtr++;
                    }
                }
                output->write((const char *)lenBuffer, (uint32_t)data_size);
                // Write actual data
                data_size = dataPtr - dataBuffer;
                //std::cout << "data_size " << data_size << std::endl;
                be_value = htobe64(data_size);
                output->write((const char *)&be_value, (uint32_t)sizeof(int64_t));
                output->write((const char *)dataBuffer, (uint32_t)data_size);
            break;
        }
    }
    if(inFrame->lastFrame){
        done = true;
    }
    inFrame->Free();
    return done;
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
