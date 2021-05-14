#ifndef DIKE_HTTP_REQUEST_HANDLER
#define DIKE_HTTP_REQUEST_HANDLER

#include <map>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

typedef std::map<std::string, std::string> DikeConfig;

class DikeHTTPRequestHandler : public Poco::Net::HTTPRequestHandler {
public:  
  int verbose = 0;
  std::map<std::string, std::string> dikeConfig;
  DikeHTTPRequestHandler(int verbose, DikeConfig & dikeConfig): Poco::Net::HTTPRequestHandler() {
    this->verbose = verbose;
    this->dikeConfig = dikeConfig;
  }
};

#endif /* DIKE_HTTP_REQUEST_HANDLER */