#include <Poco/URI.h>
#include "Poco/Thread.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/zlib.h>

#include <lz4.h>
#include <zstd.h>   

#include "LambdaNode.hpp"
#include "LambdaFilterNode.hpp"

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
        //std::cout << " LambdaParquetReader id " << threadId << " reuse FS connection "<< std::endl;
        fs = hadoopFileSystemMap[threadId];        
    } else {
        //std::cout << " LambdaParquetReader id " << threadId << " create FS connection "<< std::endl;
        arrow::io::HadoopFileSystem::Connect(&hdfsConnectionConfig, &fs);
        hadoopFileSystemMap[threadId] = fs;        
    }
                                    
    fs->OpenReadable(fileName, &inputFile);                        

    if (fileMetaDataMap.count(fileName)) {
        if (0 == fileLastAccessTimeMap[fileName].compare(dikeProcessorConfig["Configuration.LastAccessTime"])){
            //std::cout << " LambdaParquetReader reuse fileMetaData "<< std::endl;
            fileMetaData = fileMetaDataMap[fileName];                
        } else {
            //std::cout << " LambdaParquetReader read fileMetaData "<< std::endl;
            fileMetaData = std::move(parquet::ReadMetaData(inputFile));
            fileMetaDataMap[fileName] = fileMetaData;
            fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];                
        }            
    } else {
        //std::cout << " LambdaParquetReader read fileMetaData "<< std::endl;
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
        Column * col = new Column(this, i, name,  columnTypes[i]); 
        frame->Add(col);
    }    

    // This will send update request down to graph
    UpdateColumnMap(frame);

    for(int i = 0; i < columnCount; i++){
        if(frame->columns[i]->useCount > 0) {
            //std::cout << "Create reader for Column " << i << std::endl;
            columnReaders[i] = std::move(rowGroupReader->Column(i));
            frame->columns[i]->Init(); // This will allocate memory buffers
        } else {
            columnReaders[i] = NULL;
        }
    }
    freeFrame(frame); // this will put this frame on framePool

    // Create few additional frames
    for(int c = 0; c < 3; c++) {
        frame = new Frame(this); // Allocate new frame with data    
        for(int i = 0; i < columnCount; i++){                        
            auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
            std::string name = columnRoot->name();            
            Column * col = new Column(this, i, name,  columnTypes[i]);
            //std::cout << "Create Column " << i <<  " " << name << std::endl;            
            frame->Add(col);
            if(columnReaders[i]) {
                col->Init(); // This will allocate memory buffers
            }        
        }
        freeFrame(frame); // this will put this frame on framePool
    }
}

bool InputNode::Step()
{    
    //std::cout << "InputNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    int size = std::min((int)Column::MAX_SIZE, numRows - rowCount);    

    Frame * frame = allocFrame();
    if(frame == NULL){
        frame = new Frame(this); // Allocate new frame with data    
        for(int i = 0; i < columnCount; i++){                        
            auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
            std::string name = columnRoot->name();            
            Column * col = new Column(this, i, name,  columnTypes[i]);
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
        for(int i = 0; i < columnCount; i++){
            if(columnReaders[i]) {
                columnReaders[i].reset();
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
                inFrame->columns[j]->useCount ++; // This column will be in use
                columnMap[i] = j;
            }
        }
    }
    Frame * outFrame = new Frame(this);
    outFrame->columns.resize(columnCount);
    for(int i = 0; i < columnCount; i++) {
        outFrame->columns[i] = inFrame->columns[columnMap[i]]; 
    }

    Node::UpdateColumnMap(outFrame);
    freeFrame(outFrame); // this will put this frame on framePool
    
    // Create few additional frames
    for(int c = 0; c < 3; c++) {
        outFrame = new Frame(this); // Allocate new frame with data
        outFrame->columns.resize(columnCount);
        for(int i = 0; i < columnCount; i++) {
            outFrame->columns[i] = inFrame->columns[columnMap[i]];
        }
        freeFrame(outFrame); // this will put this frame on framePool
    }    
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
        outFrame->columns[i] = inFrame->columns[columnMap[i]]; 
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
    if(compressionEnabled) {
        ZSTD_Context.resize(frame->columns.size());            

        for (int i = 0; i < ZSTD_Context.size(); i++) {
            ZSTD_Context[i] = ZSTD_createCCtx();
            ZSTD_CCtx_setParameter(ZSTD_Context[i], ZSTD_c_compressionLevel, compressionLevel);
            //ZSTD_CCtx_setParameter(ZSTD_Context[i], ZSTD_c_strategy, ZSTD_fast);
        }
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
        int64_t data_size;
        switch (col->data_type) {
            case Column::DataType::INT64:
                data_size = col->row_count * sizeof(int64_t);
                TranslateBE64(col->int64_values, dataBuffer, col->row_count);
                //Send(dataBuffer, data_size, true);
                Send(i, Column::DataType::INT64, sizeof(int64_t), dataBuffer, data_size);
                break;
            case Column::DataType::DOUBLE:
                data_size = col->row_count * sizeof(double);                
                TranslateBE64(col->double_values, dataBuffer, col->row_count);
                //Send(dataBuffer, data_size, true);
                Send(i, Column::DataType::DOUBLE, sizeof(double), dataBuffer, data_size);
            break;
            case Column::DataType::BYTE_ARRAY:
                data_size = col->row_count;
                uint8_t * dataPtr = dataBuffer;
                int fixedLen = col->ba_values[0].len;
                bool fixed_len_byte_array = true;
                for(int j = 0; j < col->row_count; j++){
                    uint8_t len = col->ba_values[j].len;
                    lenBuffer[j] = len;
                    if(len != fixedLen) {
                        fixed_len_byte_array = false;
                    }
                    for(int k = 0 ; k < len; k++){
                        *dataPtr = col->ba_values[j].ptr[k];
                        dataPtr++;
                    }
                }
                if(data_size > Column::MAX_TEXT_SIZE){ // TODO we have to handle it gracefully
                    std::cout << "OutputNode::Step " << stepCount << " memory corrupted :: " << data_size << std::endl;
                }

                if(fixed_len_byte_array){
                    data_size = dataPtr - dataBuffer;
                    Send(i, Column::DataType::FIXED_LEN_BYTE_ARRAY, fixedLen, dataBuffer, data_size);                    
                } else {
                    //Send(lenBuffer, data_size, true);                
                    Send(i, Column::DataType::BYTE_ARRAY, sizeof(uint8_t), lenBuffer, data_size);
                    data_size = dataPtr - dataBuffer;
                    //Send(dataBuffer, data_size, false);
                    Send(i, Column::DataType::BYTE_ARRAY, 0, dataBuffer, data_size);
                }
            break;
        }
    }

    if(inFrame->lastFrame){
        done = true;
    }
    
    inFrame->Free();
    return done;
}

void OutputNode::Send(int id, Column::DataType data_type, int type_size, void * data, uint32_t len)
{
    int header[4];
    header[0] = htobe32(data_type); // TYPE
    header[1] = htobe32(type_size); // TYPE SIZE
    header[2] = htobe32(len); // DATA LEN
    header[3] = htobe32(0); // COMPRESSED LEN

    int64_t be_value;
    if(compressionEnabled) {
        //CompressLZ4((uint8_t *)data, len);
        CompressZSTD(id, (uint8_t *)data, len);
        header[3] = htobe32(compressedLen); // COMPRESSED LEN
        output->write((const char *)header, (uint32_t)(4*sizeof(uint32_t)));
        output->write((const char *)compressedBuffer, compressedLen);
    } else {        
        output->write((const char *)header, (uint32_t)(4*sizeof(uint32_t)));
        output->write((const char *)data, len);
    }
}

void OutputNode::TranslateBE64(void * in_data, uint8_t * out_data, uint32_t len)
{
    // Translate data to Big Endian
    int64_t * in_ptr = (int64_t *)in_data;
    int64_t * out_ptr = (int64_t *)out_data;
    for(int i = 0; i < len; i ++) {
        out_ptr[i] = htobe64(in_ptr[i]);
    }
}

void OutputNode::CompressZlib(uint8_t * data, uint32_t len, bool is_binary)
{
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    if(is_binary){
        defstream.data_type = Z_BINARY;
    } else {
        defstream.data_type = Z_TEXT;
    }
    defstream.avail_in = len; // size of input
    defstream.next_in = data; // input data
    defstream.avail_out = compressedBufferLen; // size of output
    defstream.next_out = compressedBuffer; // output data
        
    deflateInit(&defstream, Z_BEST_SPEED);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    compressedLen = defstream.total_out;
}

void OutputNode::CompressLZ4(uint8_t * data, uint32_t len)
{
    //compressedLen = LZ4_compress_default((const char*)data, (char*)compressedBuffer, len, compressedBufferLen);
    int acceleration = 1; // The larger the acceleration value, the faster the algorithm. Each successive value providing roughly +~3% to speed.
    //compressedLen = LZ4_compress_fast((const char*)data, (char*)compressedBuffer, len, compressedBufferLen, acceleration);

    compressedLen = LZ4_compress_fast_extState(lz4_state_memory, (const char*)data, (char*)compressedBuffer, len, compressedBufferLen, acceleration);        
}

void OutputNode::CompressZSTD(int id, uint8_t * data, uint32_t len)
{
    //compressedLen = ZSTD_compress( compressedBuffer, compressedBufferLen, data, len, 1);
    //compressedLen = ZSTD_compressCCtx(ZSTD_Context[id], compressedBuffer, compressedBufferLen, data, len, -5);
    compressedLen = ZSTD_compress2(ZSTD_Context[id], compressedBuffer, compressedBufferLen, data, len);
}

Node * lambda::CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
    std::string typeStr = pObject->getValue<std::string>("Type");
    if(typeStr.compare("_INPUT") == 0){
        return new InputNode(pObject, dikeProcessorConfig, output);
    }
    if(typeStr.compare("_FILTER") == 0){
        return new FilterNode(pObject, dikeProcessorConfig, output);
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
