#include <Poco/URI.h>
#include "Poco/Thread.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/zlib.h>

#include <lz4.h>
#include <zstd.h>

#include <omp.h>

#include "LambdaFileReader.hpp"

#include "LambdaNode.hpp"
#include "LambdaFilterNode.hpp"
#include "LambdaAggregateNode.hpp"

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
    std::string rpcAddress;
    for(int i = 0; i < uriParams.size(); i++){
        if(uriParams[i].first.compare("namenoderpcaddress") == 0){
            rpcAddress = uriParams[i].second;
            //std::cout << "namenoderpcaddress : " << rpcAddress << std::endl;
            auto pos = rpcAddress.find(':');
            hdfsConnectionConfig.host = rpcAddress.substr(0, pos);
            hdfsConnectionConfig.port = std::stoi(rpcAddress.substr(pos + 1, rpcAddress.length()));
        } else if(uriParams[i].first.compare("user.name") == 0) {
            hdfsConnectionConfig.user = uriParams[i].second;                
        }            
    }
    std::string path = uri.getPath();
    std::string fileName = path.substr(11, path.length()); // skip "/webhdfs/v1"    

    std::string fullPath = "hdfs://" + rpcAddress + fileName;

    //inputFile = std::move(std::shared_ptr<ReadableFile>(new ReadableFile(fullPath)));

    inputFileMutex.lock();
    if (fileMetaDataMap.count(fileName)) {
        if (0 == fileLastAccessTimeMap[fileName].compare(dikeProcessorConfig["Configuration.LastAccessTime"])){
            //std::cout << " LambdaParquetReader reuse fileMetaData "<< std::endl;
            inputFile = inputFileMap[fileName];
            fileMetaData = fileMetaDataMap[fileName];                
        } else {
            //std::cout << " LambdaParquetReader read fileMetaData "<< std::endl;
            inputFile = std::move(std::shared_ptr<ReadableFile>(new ReadableFile(fullPath)));
            inputFileMap[fileName] = inputFile;

            fileMetaData = std::move(parquet::ReadMetaData(inputFile));
            fileMetaDataMap[fileName] = fileMetaData;
            fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];                
        }            
    } else {
        //std::cout << " LambdaParquetReader read fileMetaData "<< std::endl;
        inputFile = std::move(std::shared_ptr<ReadableFile>(new ReadableFile(fullPath)));
        inputFileMap[fileName] = inputFile;

        fileMetaData = std::move(parquet::ReadMetaData(inputFile));
        fileMetaDataMap[fileName] = fileMetaData;
        fileLastAccessTimeMap[fileName] = dikeProcessorConfig["Configuration.LastAccessTime"];            
    }
    inputFileMutex.unlock();
    
    schemaDescriptor = fileMetaData->schema();
    parquet::ReaderProperties readerProperties = parquet::default_reader_properties();
    //readerProperties.enable_buffered_stream();
    readerProperties.set_buffer_size(1<<20);

    parquetFileReader = std::move(parquet::ParquetFileReader::Open(inputFile, readerProperties, fileMetaData));
    columnCount = schemaDescriptor->num_columns();
    rowGroupCount = fileMetaData->num_row_groups();
    rowGroupCount = 4;
    
    columnReaders = new std::shared_ptr<parquet::ColumnReader> [columnCount];
    columnTypes = new Column::DataType[columnCount];    
}

void InputNode::UpdateColumnMap(Frame * _unused)
{  
    Frame * frame = new Frame(this);
    for(int i = 0; i < columnCount; i++){                
        auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
        std::string name = columnRoot->name();
        parquet::Type::type physical_type = columnRoot->physical_type();
        columnTypes[i] = (Column::DataType) physical_type; // TODO type translation
        Column * col = new Column(this, i, name,  columnTypes[i]); 
        frame->Add(col);
    }
    // barrier nodes will need the info
    frame->totalRowGroups = rowGroupCount;

    // This will send update request down to graph
    Node::UpdateColumnMap(frame);
    
    for(int i = 0; i < columnCount; i++) {
        if(frame->columns[i]->useCount > 0) { // Lower nodes need this column
            columnMap.push_back(i);
            frame->columns[i]->Init(); // This will allocate memory buffers
        }
    }         

    // Create few additional frames
    for(int j = 0; j < 3; j++) {
        Frame * f = new Frame(this); // Allocate new frame with data
        for(int i = 0; i < columnCount; i++){                        
            auto columnRoot = (parquet::schema::PrimitiveNode*)schemaDescriptor->GetColumnRoot(i);
            std::string name = columnRoot->name();            
            Column * col = new Column(this, i, name,  columnTypes[i]);
            //std::cout << "Create Column " << i <<  " " << name << std::endl;            
            f->Add(col);
            if(frame->columns[i]->useCount > 0) {
                col->Init(); // This will allocate memory buffers
            }        
        }
        freeFrame(f); // this will put this frame on framePool
    }

    freeFrame(frame); // this will put this frame on framePool    
}

void InputNode::Init(int rowGroupIndex) 
{        
    std::chrono::high_resolution_clock::time_point t1;
    if(verbose){
        t1 =  std::chrono::high_resolution_clock::now();
    }    
    
    done = false;
    rowGroupReader = std::move(parquetFileReader->RowGroup(rowGroupIndex));
    numRows = rowGroupReader->metadata()->num_rows();
    rowCount = 0;
    //#pragma omp parallel for num_threads(4)
    for(int i = 0; i < columnMap.size(); i++) {
        int c = columnMap[i];
        //std::cout << "Create reader for Column " << c  << std::endl;
        columnReaders[c] = std::move(rowGroupReader->Column(c));        
    }        

    if(verbose){
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> run_time = t2 - t1;   
        std::cout << "InputNode::Init " << rowGroupIndex << " took " << run_time.count()/ 1000 << " sec" << std::endl;
    }    
}

bool InputNode::Step()
{        
    //std::cout << "InputNode::Step " << stepCount << std::endl;
    if(done) { return done; }
    stepCount++;

    int size = std::min((int)Column::MAX_SIZE, numRows - rowCount);    
    Frame * frame = allocFrame();

    std::chrono::high_resolution_clock::time_point t1;
    if(verbose){
        t1 =  std::chrono::high_resolution_clock::now();
    }

    //#pragma omp parallel
    {        
        #pragma omp parallel for num_threads(4)
        for(int i = 0; i < columnMap.size(); i++) {            
            int col = columnMap[i];
            frame->columns[col]->Read(columnReaders[col], size);
        }        
    }

    rowCount += size;
    if(rowCount >= numRows){
        frame->lastFrame = true;
        done = true;
    } else {
        frame->lastFrame = false;
    }
    nextNode->putFrame(frame); // Send frame down to graph

    if(verbose){
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        runTime += t2 - t1;
    }
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

    std::chrono::high_resolution_clock::time_point t1;
    if(verbose){
        t1 =  std::chrono::high_resolution_clock::now();
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

    if(verbose){    
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();
        runTime += t2 - t1;    
    }
    return done;
}

void OutputNode::UpdateColumnMap(Frame * frame) 
{
    nCol = frame->columns.size();
    uint32_t count = 0;

    schema[count ++] = htobe64(nCol);

    for(int i  = 0; i < nCol; i++){
        schema[count ++] = htobe64(frame->columns[i]->data_type);
    }    
}

void OutputNode::Init(int rowGroupIndex)
{
    done = false;
    output->write((const char *)schema, (nCol + 1)*sizeof(int64_t));

    std::cout << "OutputNode::Init nCol " << nCol << std::endl;
    if(compressionEnabled && !initialized) {
        ZSTD_Context.resize(nCol);            

        for (int i = 0; i < ZSTD_Context.size(); i++) {
            ZSTD_Context[i] = ZSTD_createCCtx();
            ZSTD_CCtx_setParameter(ZSTD_Context[i], ZSTD_c_compressionLevel, compressionLevel);
            //ZSTD_CCtx_setParameter(ZSTD_Context[i], ZSTD_c_strategy, ZSTD_fast);
        }
    }
    initialized = true;
}

bool OutputNode::Step()
{        
    if(done) { return done; }
    
    Frame * inFrame = getFrame();
    if(inFrame == NULL) return done;

    //std::cout << "OutputNode::Step " << stepCount << " Rows " << inFrame->columns[0]->row_count << std::endl;
    stepCount++;
    recordsOut += inFrame->columns[0]->row_count;
    
    std::chrono::high_resolution_clock::time_point t1;
    if(verbose){
        t1 =  std::chrono::high_resolution_clock::now();
    }

    Column * col = 0;
    int64_t be_value;
    for( int i  = 0; i < inFrame->columns.size(); i++){
        col = inFrame->columns[i];
        if(col->row_count == 0){
            continue;
        }
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
    if(verbose){
        std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();        
        runTime += t2 - t1;
    }
    return done;
}

void OutputNode::Send(int id, Column::DataType data_type, int type_size, void * data, uint32_t len)
{
    int64_t be_value;
    if(compressionEnabled && len > 1024) { // There is no need to compress small chunks of data
        int * header = (int *)compressedBuffer;
        header[0] = htobe32(data_type); // TYPE
        header[1] = htobe32(type_size); // TYPE SIZE
        header[2] = htobe32(len); // DATA LEN
        header[3] = htobe32(0); // COMPRESSED LEN
        uint8_t * data_out = compressedBuffer + HEADER_LEN;
        
        CompressZSTD(id, (uint8_t *)data, len, data_out);
        header[3] = htobe32(compressedLen); // COMPRESSED LEN
        
        output->write((const char *)compressedBuffer, compressedLen + HEADER_LEN);
    } else {        
        int * header = (int *)((uint8_t *)data - HEADER_LEN);
        header[0] = htobe32(data_type); // TYPE
        header[1] = htobe32(type_size); // TYPE SIZE
        header[2] = htobe32(len); // DATA LEN
        header[3] = htobe32(0); // COMPRESSED LEN
        
        output->write((const char *)header, len + HEADER_LEN);
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

void OutputNode::CompressZSTD(int id, uint8_t * data_in, uint32_t len, uint8_t * data_out)
{
    //compressedLen = ZSTD_compress( compressedBuffer, compressedBufferLen, data, len, 1);
    //compressedLen = ZSTD_compressCCtx(ZSTD_Context[id], compressedBuffer, compressedBufferLen, data, len, -5);
    compressedLen = ZSTD_compress2(ZSTD_Context[id], data_out, compressedBufferLen, data_in, len);
}

Node * lambda::CreateNode(Poco::JSON::Object::Ptr pObject, DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) {
    std::string typeStr = pObject->getValue<std::string>("Type");
    if(typeStr.compare("_INPUT") == 0){
        return new InputNode(pObject, dikeProcessorConfig, output);
    }
    if(typeStr.compare("_FILTER") == 0){
        return new FilterNode(pObject, dikeProcessorConfig, output);
    }
    if(typeStr.compare("_AGGREGATE") == 0){
        return new AggregateNode(pObject, dikeProcessorConfig, output);
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
std::map< std::string, std::shared_ptr<ReadableFile> >  lambda::InputNode::inputFileMap;
std::mutex  lambda::InputNode::inputFileMutex;


