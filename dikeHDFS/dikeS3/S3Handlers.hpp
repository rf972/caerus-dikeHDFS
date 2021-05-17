#ifndef S3_HANDLERS_HPP
#define S3_HANDLERS_HPP

#include <iostream>
#include <string>
#include <map>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include "DikeHTTPRequestHandler.hpp"

class ListObjectsV2 : public DikeHTTPRequestHandler {
public:
   ListObjectsV2(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~ListObjectsV2() {};
};

class SelectObjectContent : public DikeHTTPRequestHandler {
public:
   SelectObjectContent(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~SelectObjectContent() {};
private:
   void readFromHdfs(std::map<std::string, std::string> readParamMap, std::ostream & toClient);
};

#endif /* S3_HANDLERS_HPP */