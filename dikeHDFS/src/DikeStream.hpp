#ifndef DIKE_STREAM_H
#define DIKE_STREAM_H

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPServerSession.h>

#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/HTTPHeaderStream.h"

#include <iostream>

#include "DikeIO.hpp"

using namespace Poco::Net;

class DikeHDFSSession : public HTTPClientSession {
  public:
  DikeHDFSSession(const std::string& host, Poco::UInt16 port = HTTPSession::HTTP_PORT)
  :HTTPClientSession(host,port) { }
  
  int readResponseHeader(HTTPResponse& response) {
    flushRequest();
   
    do {
      response.clear();
      HTTPHeaderInputStream his(*this);
      try {
        response.read(his);
      } catch (...) {
        close();
        std::cout << "DikeHDFSSession::readResponseHeader Exception" << std::endl;
      }
    } while (response.getStatus() == HTTPResponse::HTTP_CONTINUE);
        
    return response.getStatus();
  }

  int read(char * buf, uint32_t size) {
    return HTTPClientSession::read(buf, (std::streamsize)size);
  }
};

class DikeInSession : public DikeIO {
  public:
  DikeHDFSSession * inSession = NULL;

  DikeInSession(DikeHDFSSession * inSession) {
    this->inSession = inSession;
  }

  ~DikeInSession(){ }
  virtual int write(const char * buf, uint32_t size) {
    return -1;
  }

  virtual int read(char * buf, uint32_t size) {
    if(inSession) {
      int n = 0;
      int len = size;
      try {
        while( len > 0 ) {
            int i = inSession->read((char*)&buf[n], len );            
            n += i;
            len -= i;
            if(i <= 0){
                readBytes += n;
                return n;
            }
        }
      } catch (...) {
        std::cout << "DikeInSession: Caught exception " << std::endl;
      }

      readBytes += n;
      return n;
    }
    
    return -1;
  }
};

class DikeOut : public DikeIO {
  public:
  Poco::Net::StreamSocket * outSocket = NULL;

  DikeOut(Poco::Net::StreamSocket * outSocket){
    this->outSocket = outSocket;
  }

  ~DikeOut(){ }
  virtual int read(char * buf, uint32_t size) {
    return -1;
  }

  virtual int write(const char * buf, uint32_t size) {
    int len = size;
    int n = 0;
    if(outSocket) {
      try {
        while(len > 0) {
            int i = outSocket->sendBytes(&buf[n], len, 0);
            n += i;
            len -= i;
            if(i <= 0){
              return n;
            }
        }
        return n; 
      } catch (...) {
        std::cout << "DikeOut: Caught exception " << std::endl;
      }
    }
    return -1;
  }
};

#endif /* DIKE_STREAM_H */
