#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Poco/Net/HTTPClientSession.h>

#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <iostream>

//#include "S3Handlers.hpp"
#include "DikeUtil.hpp"
#include "DikeSQL.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

class NameNodeHandler : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
   {
    cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN Start " << DikeUtil().Reset() << endl;
    std::istream& fromClient = req.stream();
    HTTPRequest hdfs_req((HTTPRequest)req);
    
    string host = req.getHost();    
    host = host.substr(0, host.find(':'));
    hdfs_req.setHost(host, 9870);

    cout << hdfs_req.getURI() << endl;     
    hdfs_req.write(cout);

    HTTPClientSession session(host, 9870);
    std::ostream& toHDFS =session.sendRequest(hdfs_req);

    Poco::StreamCopier::copyStream(fromClient, toHDFS, 8192);  

    HTTPResponse hdfs_resp;

    std::istream& fromHDFS = session.receiveResponse(hdfs_resp);
    
    (HTTPResponse &)resp = hdfs_resp;    

    if(resp.has("Location")) {
      //cout << DikeUtil().Blue() << resp.get("Location") << DikeUtil().Reset() << endl;
      string location = resp.get("Location");
      location.replace(location.find(":9864"), 5, ":9859");
      resp.set("Location", location);
      //cout << DikeUtil().Blue() << resp.get("Location") << DikeUtil().Reset() << endl;
    }

    resp.write(cout);
    ostream& toClient = resp.send();
    Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
    toClient.flush();
  
    cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN End " << DikeUtil().Reset() << endl;
   }   
};

class DataNodeHandler : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
   {
    cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN Start " << DikeUtil().Reset() << endl;
    HTTPRequest hdfs_req((HTTPRequest)req);

    string host = req.getHost();    
    host = host.substr(0, host.find(':'));
    hdfs_req.setHost(host, 9864);

    cout << hdfs_req.getURI() << endl;     
    hdfs_req.write(cout);    

    HTTPClientSession session(host, 9864);
    session.sendRequest(hdfs_req);
    HTTPResponse hdfs_resp;

    std::istream& fromHDFS = session.receiveResponse(hdfs_resp);    
    (HTTPResponse &)resp = hdfs_resp;    

    resp.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);
    resp.setChunkedTransferEncoding(true);    
    resp.setKeepAlive(true);

    resp.write(cout);

    ostream& toClient = resp.send();

    if(req.has("ReadParam")) {
      string readParam = req.get("ReadParam");
      cout << DikeUtil().Blue();      

      std::istringstream readParamStream(readParam.c_str());      
      std::istream& xmlStream(readParamStream);
      AbstractConfiguration *cfg = new XMLConfiguration(xmlStream);            
      cout << "Name: " << cfg->getString("Name") << endl;
      cout << "Schema: " << cfg->getString("Configuration.Schema") << endl;
      cout << "Query: " << cfg->getString("Configuration.Query") << endl;
      cout << DikeUtil().Reset() << endl;
      DikeSQLParam dikeSQLParam;
      dikeSQLParam.schema = cfg->getString("Configuration.Schema");
      dikeSQLParam.query = cfg->getString("Configuration.Query");
      DikeSQL::Run(fromHDFS, toClient, &dikeSQLParam);
    } else {
      Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
    }
    
    toClient.flush();

    cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN End " << DikeUtil().Reset() << endl;
   }   
};

class NameNodeHandlerFactory : public HTTPRequestHandlerFactory {
public:
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new NameNodeHandler;
  }
};

class DataNodeHandlerFactory : public HTTPRequestHandlerFactory {
public:
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new DataNodeHandler;
  }
};

class DikeServerApp : public ServerApplication
{
protected:
  int main(const vector<string> &)
  {
    HTTPServer nameNode(new NameNodeHandlerFactory, ServerSocket(9860), new HTTPServerParams);
    HTTPServer dataNode(new DataNodeHandlerFactory, ServerSocket(9859), new HTTPServerParams);

    nameNode.start();
    dataNode.start();

    cout << endl << "Server started" << endl;

    waitForTerminationRequest();  // wait for CTRL-C or kill

    cout << endl << "Shutting down..." << endl;
    nameNode.stop();
    dataNode.stop();

    return Application::EXIT_OK;
  }
};

int main(int argc, char** argv)
{  
  DikeServerApp app;
  return app.run(argc, argv);
}