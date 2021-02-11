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

    int validate(){
        assert(posPtr <= endPtr);
        assert(posPtr >= startPtr);
        if(memoryOwner){
            for(int i = 0; i < DIKE_BUFFER_GUARD; i++){
                assert(endPtr[i] == 0);
            }
        }
#if _DEBUG
        std::size_t found = std::string((char *)startPtr, posPtr - startPtr).find('|');
        assert(std::string::npos == found);
#endif        
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
            if(col < data_count -1){
                *p = delim;
                p++;
            }
        }
        *p = term;
        p++;
        bytes = p - posPtr;
        posPtr = p;
        return bytes;
    }

    int write(const char * data, char delim){
        uint8_t * d = (uint8_t *)data;
        uint8_t * p = posPtr;
        int rc;
        while(*d != 0 && p < endPtr/* && *d != delim*/){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr){ // out of space new buffer needed
            return 0;
        }

        if(*d == 0){ // end of data
            *p = delim;
            p++;
            rc = p - posPtr;
            posPtr = p;
#if _DEBUG
            std::size_t found = std::string(data, rc).find('|');
            assert(std::string::npos == found);
#endif
            return rc;
        }

        if(*d != delim){ // sanity check
            std::cout << "Something is very wrong there" << std::endl;
            return 0;
        }
        std::cout << "Something is very wrong there" << std::endl;
        // We need to wrap delimiter in quotations
        d = (uint8_t *)data;
        p = posPtr;
        *p = '"';
        p++;
        while(*d != 0 && p < endPtr){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr-2){ // out of space new buffer needed
            return 0;
        }
        *p = '"';
        p++;
        *p = delim;
        p++;
        rc = p - posPtr;
        posPtr = p;
#if _DEBUG
        std::size_t found = std::string(data, rc).find('|');
        assert(std::string::npos == found);
#endif
        return rc;
    }

    int write(const char * data, char delim, char term){         
        uint8_t * d = (uint8_t *)data;
        uint8_t * p = posPtr;
        int rc;
        while(*d != 0 && p < endPtr/* && *d != delim*/){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr){ // out of space new buffer needed
            return 0;
        }

        if(*d == 0){ // end of data
            *p = term;
            p++;
            rc = p - posPtr;
            posPtr = p;
            return rc;
        }

        if(*d != delim){ // sanity check
            std::cout << "Something is very wrong there" << std::endl;
            return 0;
        }

        std::cout << "Something is very wrong there" << std::endl;
        // We need to wrap delimiter in quotations
        d = (uint8_t *)data;
        p = posPtr;
        *p = '"';
        p++;
        while(*d != 0 && p < endPtr){
            *p = *d;
            p++;
            d++;
        }

        if(p >= endPtr-2){ // out of space new buffer needed
            return 0;
        }
        *p = '"';
        p++;
        *p = term;
        p++;
        rc = p - posPtr;
        posPtr = p;
        return rc;
    }

    int write(char term){         
        uint8_t * p = posPtr;
        if( p < endPtr){
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

    std::streamsize getSize(){
        assert(posPtr <= endPtr);
        int rc = posPtr - startPtr;
        //assert(rc > 0);
        return std::streamsize(rc);
    }
};

#endif /* DIKE_BUFFER_HPP */