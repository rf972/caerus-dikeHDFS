#ifndef DIKE_BUFFER_HPP
#define DIKE_BUFFER_HPP

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

#include "DikeUtil.hpp"

class DikeBuffer{
    public:
    uint8_t *startPtr;
    uint8_t *posPtr;
    uint8_t *endPtr;
    uint32_t size;
    uint32_t id;

    bool memoryOwner = false;
    enum {
        DIKE_BUFFER_GUARD = 4
    };
    DikeBuffer(int size){
        this->size = size;
        startPtr = (uint8_t *)malloc(size + DIKE_BUFFER_GUARD);
        memoryOwner = true;
        //std::cout << "Allocating "  << size << " bytes" << std::endl;
        posPtr = startPtr;
        endPtr = startPtr + size;
        for(int i = 0; i < DIKE_BUFFER_GUARD; i++){
            endPtr[i] = 0;
        }
    }

    DikeBuffer(uint8_t * buffer, int size){
        this->size = size;
        startPtr = buffer;
        //std::cout << "Allocating "  << size << " bytes" << std::endl;
        posPtr = startPtr;
        endPtr = startPtr + size;
        memoryOwner = false;
    }

    ~DikeBuffer(){
        //std::cout << "~DikeBuffer" << std::endl;
        if(memoryOwner){
            free(startPtr);
        }
        startPtr = 0;
    }

    void validateBeforeWrite() {
        // Check if we have zeroes in the buffer
        for(int i = 0; i < getSize(); i++){
            if(startPtr[i] == 0){
                std::cout << "Detected zero at pos : " << i << std::endl;
            }
        }

        // Check if we have two consecutive terminations
        for(int i = 1; i < getSize(); i++){
            if(startPtr[i-1] == '\n' && startPtr[i] == '\n'){
                std::cout << "Detected consecutive terminations at pos : " << i << std::endl;
            }
        }

    }

    void setReadableBytes(int length)
    {
        endPtr = startPtr + length;   
    }

    int64_t write(const void* data, int64_t nbytes) {
        //std::cout << "DikeBuffer::write " << nbytes << " size " << getSize() << std::endl;
        if(endPtr - posPtr < nbytes + 1){
            std::cout << "DikeBuffer::write need new buffer" << (endPtr - posPtr) << std::endl;
            return 0;
        }        
        memcpy(posPtr, data, nbytes);
        posPtr += nbytes;
        return nbytes;
    }

    int write(const char **res, int data_count, char delim, char term, int total_bytes) 
    {
        int col = data_count;
        uint8_t * d = NULL;
        uint8_t * p = posPtr;
        int bytes = 0;

        /* Check that data can fit */
        if(endPtr - posPtr < total_bytes + 1){
            return 0;
        }

        for(col = 0; col < data_count; col++){
            d = (uint8_t *)res[col];
            while(*d != 0) {
                *p = *d;
                p++;
                d++;
            }            
            *p = delim;
            p++;            
        }

        *(p - 1) = term;        
        bytes = p - posPtr;
        posPtr = p;
        return bytes;
    }

    int write(char term){         
        uint8_t * p = posPtr;
        if( p < endPtr - 2){
            *p = term;
            p++;
            posPtr = p;
            return 1;
        } else {
            return 0;
        }
    }

    void reset(){
        assert(posPtr <= endPtr);
        posPtr = startPtr;
    }

    int getSize(){
        assert(posPtr <= endPtr);
        int rc = posPtr - startPtr;
        //assert(rc > 0);
        return rc;
    }
};

#endif /* DIKE_BUFFER_HPP */