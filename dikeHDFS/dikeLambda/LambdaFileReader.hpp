#ifndef LAMBDA_FILE_READER_HPP
#define LAMBDA_FILE_READER_HPP

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/filesystem/filesystem.h>

#include <orc/OrcFile.hh>

namespace lambda {

class ReadableFile : public arrow::io::RandomAccessFile {    
 public:
    std::shared_ptr<arrow::Buffer> buffer;   
    std::unique_ptr<orc::InputStream> stream;
    uint64_t fileLength;    
    arrow::MemoryPool* pool_ = arrow::default_memory_pool();

    ReadableFile(std::string path) {
        //std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
        //stream = orc::readFile(path);
        stream = orc::readHdfsFile(path);
        fileLength =  static_cast<uint64_t>(stream->getLength());
    }

    ~ReadableFile() override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;      
    }

    arrow::Status Close() override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return arrow::Status::OK();
    }

    bool closed() const override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return false;
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return 0;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return std::move(buffer);
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;
      stream->read(out, nbytes, position);      
      return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        if(position + nbytes > fileLength){
            std::cout << " Invalid read size " << nbytes << std::endl;
        }
        //std::cout << __FUNCTION__ << " : " << __LINE__ << " bytes " << nbytes << " offset " << position << std::endl;        
        ARROW_ASSIGN_OR_RAISE(auto buffer, ::arrow::AllocateResizableBuffer(nbytes, pool_));
        stream->read( buffer->mutable_data(), nbytes, position);
        return std::move(buffer);         
    }

    arrow::Status Seek(int64_t position) override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override { 
      std::cout << __FUNCTION__ << " : " << __LINE__ << std::endl;
      return 0;
    }

    arrow::Result<int64_t> GetSize() override { 
      //std::cout << __FUNCTION__ << " : " << __LINE__ << " return " << fileLength << std::endl;
      return fileLength;
    }
};


} // namespace lambda
#endif /* LAMBDA_FILE_READER_HPP */