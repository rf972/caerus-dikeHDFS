#ifndef DIKE_IO_HPP
#define DIKE_IO_HPP

#include <cstdint>

class DikeIO {
    public:
    uint64_t readBytes = 0;
    uint64_t writeBytes = 0;
    DikeIO(){ }
    ~DikeIO(){ }
    virtual int write(const char * buf, uint32_t size)  =  0;
    virtual int read(char * buf, uint32_t size)  =  0;    
};

#endif /* DIKE_IO_HPP */