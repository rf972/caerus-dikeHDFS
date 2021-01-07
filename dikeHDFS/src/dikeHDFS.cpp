#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include "Poco/StreamCopier.h"

#include <iostream>

//#include "S3Handlers.hpp"
#include "DikeUtil.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

class ProxyHandler : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
   {
    cout << DikeUtil().Yellow() << DikeUtil().Now() << " Start " << DikeUtil().Reset();
    cout << req.getURI() << endl;
     
    req.write(cout);
    istream& istr = req.stream();
    Poco::StreamCopier::copyStream(istr, std::cout);
    cout << endl;

    HTTPRequest hdfs_req((HTTPRequest)req);
    hdfs_req.write(cout);
   }
   
};

class DikeRequestHandlerFactory : public HTTPRequestHandlerFactory {
public:
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {

    return new ProxyHandler;
  }
};

class DikeServerApp : public ServerApplication
{
protected:
  int main(const vector<string> &)
  {
    HTTPServer s(new DikeRequestHandlerFactory, ServerSocket(9860), new HTTPServerParams);

    s.start();
    cout << endl << "Server started" << endl;

    waitForTerminationRequest();  // wait for CTRL-C or kill

    cout << endl << "Shutting down..." << endl;
    s.stop();

    return Application::EXIT_OK;
  }
};

int main(int argc, char** argv)
{  
  DikeServerApp app;
  return app.run(argc, argv);
}