#ifndef DIKE_CSV_READER_HPP
#define DIKE_CSV_READER_HPP

#include <iostream>
#include <chrono> 
#include <ctime>
#include <string>
#include <sstream> 
#include <iomanip>
#include <thread>
#include <queue>
#include <mutex>
#include <cassert>
#include <semaphore.h>
#include <unistd.h>
#include <string.h>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerResponseImpl.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPServerSession.h>

#include <Poco/URI.h>

#include "DikeUtil.hpp"
#include "DikeBuffer.hpp"
#include "DikeIO.hpp"
#include "DikeStream.hpp"

#include "DikeAsyncReader.hpp"

class DikeCsvReader: public DikeAsyncReader {
    public:
    DikeHDFSSession * hdfs_session;
    DikeInSession * dikeInSession;
    DikeIO * input = NULL;
    uint8_t * memPool = 0;
    uint64_t blockSize = 0; /* Limit reads to one HDFS block */
    uint64_t blockOffset = 0; /* If not zero we need to seek for record */   
    std::string schema;
    int columnCount = 0;

    DikeCsvReader(DikeProcessorConfig & dikeSQLConfig) {        
        std::stringstream ss;
        ss.str(dikeSQLConfig["Request"]);
        Poco::Net::HTTPRequest hdfs_req;
        hdfs_req.read(ss);

        std::string host = hdfs_req.getHost();    
        host = host.substr(0, host.find(':'));
        hdfs_req.setHost(host, std::stoi(dikeSQLConfig["dfs.datanode.http-port"]));
        hdfs_session = new DikeHDFSSession(host, std::stoi(dikeSQLConfig["dfs.datanode.http-port"]));
        hdfs_session->sendRequest(hdfs_req);

        HTTPResponse hdfs_resp;
        hdfs_session->readResponseHeader(hdfs_resp);
        dikeInSession = new DikeInSession(hdfs_session);

        Poco::URI uri = Poco::URI(hdfs_req.getURI());
        Poco::URI::QueryParameters uriParams = uri.getQueryParameters();
        dikeSQLConfig["BlockOffset"] = "0";
        for(int i = 0; i < uriParams.size(); i++){        
            if(uriParams[i].first == "offset"){
               dikeSQLConfig["BlockOffset"] = uriParams[i].second;
            }
        }        
        dikeCsvReaderInit(dikeInSession, dikeSQLConfig);
    }

    enum{
        QUEUE_SIZE  = 4,
        BUFFER_SIZE = (128 << 10)
    };
   
    int bytesRead = 0; /* How many bytes did we read so far */
    int fDelim = ','; /* Field delimiter */
    int rDelim = '\n'; /* Record Delimiter */
    int qDelim = '\"'; /* Quotation Delimiter */
 
    //DikeRecord * record = NULL; /* Single record */

    std::queue<DikeBuffer * > work_q;
    std::queue<DikeBuffer * > free_q;
    std::queue<DikeBuffer * > tmp_q;
    std::mutex q_lock;
    sem_t work_sem;
    sem_t free_sem;    
    std::thread workerThread;
    bool isRunning;
    DikeBuffer * buffer = NULL;
    int pushCount = 0;
    int emptyCount = 0;
    uint64_t recordCount = 0;

    void dikeCsvReaderInit(DikeIO * input, DikeProcessorConfig & dikeSQLConfig){
        this->input = input;        
        this->blockSize = std::stoull(dikeSQLConfig["Configuration.BlockSize"]);
        
        memPool = (uint8_t *)malloc(QUEUE_SIZE * BUFFER_SIZE);
        for(int i = 0; i < QUEUE_SIZE; i++){
            DikeBuffer * b = new DikeBuffer(&memPool[i * BUFFER_SIZE], BUFFER_SIZE);
            b->id = i;
            free_q.push(b);
        }

        sem_init(&work_sem, 0, 0);
        sem_init(&free_sem, 0, QUEUE_SIZE);
               
        isRunning = true;
        workerThread = startWorker(); // This will start reading immediatelly
        buffer = getBuffer();

        //std::stoull(uriParams[i].second)
        if(std::stoull(dikeSQLConfig["BlockOffset"]) > 0) {
            seekRecord();
        } else if (dikeSQLConfig["Configuration.HeaderInfo"].compare("IGNORE") == 0) { // USE , IGNORE or NONE
            seekRecord();
        }

        initColumnCount();
        initScema();
        initRecord();
    }

    virtual ~DikeCsvReader(){
        isRunning = false;
        //std::cout << "~DikeCsvReader" << std::endl;
        sem_post(&free_sem);
        workerThread.join();
        sem_destroy(&work_sem);
        sem_destroy(&free_sem);

        delete buffer;
        DikeBuffer * b;
        while(!free_q.empty()){
            b = free_q.front();
            free_q.pop();
            delete b;
        }
        while(!work_q.empty()){
            b = work_q.front();
            work_q.pop();
            delete b;
        }
        while(!tmp_q.empty()){
            b = tmp_q.front();
            tmp_q.pop();
            delete b;
        }        
        if(record){
            delete record;
        }
        if(memPool){
            free(memPool);
        }

        delete dikeInSession;
        delete hdfs_session;
        //std::cout << "~DikeCsvReader Push count: " << pushCount << " Empty count: " << emptyCount << " bytesRead " << bytesRead << std::endl;        
    }    

    int initRecord() {
        record = new DikeRecord(columnCount);
        return (record != NULL);
    }

    bool isEOF() {
        if(blockSize > 0 && bytesRead > blockSize) {
            return true;
        }
        return false;
    }
    
    int seekRecord() {
        uint8_t * posPtr = buffer->posPtr;        
        bool underQuote = false;
        while(posPtr < buffer->endPtr) {
            if(*posPtr == qDelim){
                //underQuote = !underQuote;
            }
            if(!underQuote){
                if(*posPtr == rDelim){
                    bytesRead += posPtr - buffer->posPtr + 1;
                    buffer->posPtr = posPtr + 1; // Skiping delimiter            
                    return 0;
                }
            }
            posPtr++;
        }        

        std::cout << "DikeCsvReader failed seek " << std::endl;
        return 1;
    }
    
    virtual int getColumnCount() {
        return columnCount;
    }

    virtual const std::string &  getSchema() {
        return schema;
    }

    virtual int getColumnValue(int col, void ** value, int * len, sqlite_aff_t * affinity) {
        *affinity = SQLITE_AFF_TEXT_TERM;
        *value = record->fields[col];
        *len = record->len[col];
        return 0;
    }

    void initScema() {
        schema = "";
        for(int i = 0; i < columnCount; i++) {
            schema += "_" +  std::to_string(i+1);
            if (i < columnCount -1 ){
                schema += ",";
            }
        }
    }

    void initColumnCount() {
        int nCol = 0;
        uint8_t * posPtr = buffer->posPtr;
        bool underQuote = false;

        while(posPtr < buffer->endPtr) {
            if(*posPtr == qDelim){
                underQuote = !underQuote;
            }
            if(!underQuote){
                if(*posPtr == fDelim){
                    nCol++;
                } else if (*posPtr == rDelim) {
                    nCol++;
                    break;
                }
            }
            posPtr++;
        }

       // std::cout << "DikeCsvReader detected " << nCol << " columns" << std::endl;
        columnCount = nCol;
    }

    virtual int readRecord() {
        if(isEOF()){
            //std::cout << "DikeCsvReader EOF at " << bytesRead << std::endl;
            return 1;
        }
        releaseBuffers();

        recordCount++;
        for(int i = 0; i < record->nCol; i++) {
            record->fields[i] = NULL;
            if(readField(i)){
                //std::cout << "DikeCsvReader EOF at " << bytesRead << std::endl;
                return 1;
            }
        }

        return 0;
    }    

    int readField(int pos) {
        uint8_t * posPtr = buffer->posPtr;
        DikeBuffer * orig_buffer = buffer;
        bool underQuote = false;
        record->fields[pos] = buffer->posPtr;

        while(posPtr < buffer->endPtr) {
            if(*posPtr == qDelim){
                underQuote = !underQuote;
            }
            if(!underQuote){
                if(*posPtr == fDelim || *posPtr == rDelim){                                                      
                    record->len[pos] = posPtr - record->fields[pos] + 1;
                    *posPtr = 0; // SQLite expects strings to be NULL terminated
                    posPtr++;
                    bytesRead += posPtr - buffer->posPtr;
                    buffer->posPtr = posPtr;
                    return 0;
                }
            }
            posPtr++;
        }

        // Use internal memory
        int count = 0;
        uint8_t * fieldPtr = record->fieldMemory[pos];
        record->fields[pos] = fieldPtr;
        // Copy first part
        posPtr = buffer->posPtr;
        while( posPtr < buffer->endPtr) {
            *fieldPtr = *posPtr;
            posPtr++;
            fieldPtr++;
            count++;
        }
        // Get new buffer
        holdBuffer(buffer);
        buffer = getBuffer();
        posPtr = buffer->posPtr;
        // Copy second part
        while(posPtr < buffer->endPtr) {            
            if(*posPtr == qDelim){
                underQuote = !underQuote;
            }
            if(!underQuote){
                if(*posPtr == fDelim || *posPtr == rDelim){
                    count++;
                    *fieldPtr = 0; // SQLite expects strings to be NULL terminated
                    posPtr++;
                    fieldPtr++;

                    record->len[pos] = count;                    
                    bytesRead += count;
                    buffer->posPtr = posPtr;
                    return 0;
                }
            }
            count++;
            *fieldPtr = *posPtr;
            posPtr++;
            fieldPtr++;
        }
        
        //std::cout << "DikeCsvReader End of data 2 at  " << bytesRead << std::endl;
        return 1;
    }

    void holdBuffer(DikeBuffer * buf) {
        //q_lock.lock();       
        tmp_q.push(buf);        
        //q_lock.unlock();        
    }

    void releaseBuffers(void) {
        DikeBuffer * b;
        //q_lock.lock();       
        while(!tmp_q.empty()){
            b = tmp_q.front();
            tmp_q.pop();
            pushBuffer(b);
        }
        //q_lock.unlock();        
    }

    void pushBuffer(DikeBuffer * buf) {        
        q_lock.lock();
        pushCount ++; // We processed this buffer
        if(free_q.empty()){
            emptyCount++; // We reading faster than processing
        }
        free_q.push(buf);        
        q_lock.unlock();
        sem_post(&free_sem);
    }

    DikeBuffer * getBuffer(){
        sem_wait(&work_sem);
        q_lock.lock();
        DikeBuffer * b = work_q.front();
        work_q.pop();
        q_lock.unlock();
        //std::cout << "DikeCsvReader buffer " << b->id << " retrieved " << std::endl;
        return b;
    }

    std::thread startWorker() {
        return std::thread([=] { Worker(); });
    }

    void Worker() {
        pthread_t thread_id = pthread_self();
        pthread_setname_np(thread_id, "DikeCsvReader::Worker");

        while(1){
            sem_wait(&free_sem);
            if(isEOF()){
                //std::cout << "DikeCsvReader EOF exiting worker thread" << std::endl;
                return;                
            }
            if(!isRunning){
                //std::cout << "DikeCsvReader not running is set exiting " << std::endl;
                return;
            }

            q_lock.lock();
            if(free_q.empty()){
                std::cout << "DikeCsvReader exiting worker thread" << std::endl;
                q_lock.unlock();
                return;
            }
            DikeBuffer * b = free_q.front();             
            free_q.pop();
            q_lock.unlock();

            b->reset();

            int n = input->read((char *)b->startPtr, BUFFER_SIZE);
            b->setReadableBytes(n);            
            if(n < BUFFER_SIZE && n > 0){
                memset((char*)&b->startPtr[n], 0, BUFFER_SIZE - n);
            }
            //std::cout << "DikeCsvReader buffer " << b->id << " ready " << std::endl;
            q_lock.lock();
            work_q.push(b);            
            q_lock.unlock();
            sem_post(&work_sem);
        }
    }
};

#endif /* DIKE_CSV_READER_HPP */