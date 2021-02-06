#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServerResponseImpl.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPServerSession.h>

#include "Poco/Net/StreamSocket.h"

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
      DikeSQLParam dikeSQLParam;

      cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN Start " << DikeUtil().Reset() << endl;
      HTTPRequest hdfs_req((HTTPRequest)req);

      string host = req.getHost();    
      host = host.substr(0, host.find(':'));
      hdfs_req.setHost(host, 9864);

      cout << hdfs_req.getURI() << endl;
      Poco::URI uri(hdfs_req.getURI());
      Poco::URI::QueryParameters uriParams = uri.getQueryParameters();
      dikeSQLParam.blockOffset = 0;
      for(int i = 0; i < uriParams.size(); i++){        
        if(uriParams[i].first == "offset"){
          cout << uriParams[i].first << ": " << uriParams[i].second << endl;
          dikeSQLParam.blockOffset = std::stoull(uriParams[i].second);
        }
      }

      hdfs_req.write(cout);    

      HTTPClientSession hdfs_session(host, 9864);
      hdfs_session.sendRequest(hdfs_req);
      HTTPResponse hdfs_resp;

      std::istream& fromHDFS = hdfs_session.receiveResponse(hdfs_resp);
      //std::istream * fromHDFS = NULL;
      //hdfs_session.readResponse(hdfs_resp);
      (HTTPResponse &)resp = hdfs_resp;
      cout << "From HDFS we got \"" << hdfs_resp.getTransferEncoding() << "\" TransferEncoding" << endl;

      if(req.has("ReadParam")) {
        resp.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);
        resp.setChunkedTransferEncoding(false);    
        resp.setKeepAlive(false);

        string readParam = req.get("ReadParam");
        cout << DikeUtil().Blue();      
        DikeSQL dikeSQL;

        try{
          std::istringstream readParamStream(readParam.c_str());      
          std::istream& xmlStream(readParamStream);
          AbstractConfiguration *cfg = new XMLConfiguration(xmlStream);            
          cout << "Name: " << cfg->getString("Name") << endl;
          cout << "Schema: " << cfg->getString("Configuration.Schema") << endl;
          cout << "Query: " << cfg->getString("Configuration.Query") << endl;
          cout << "BlockSize: " << cfg->getString("Configuration.BlockSize") << endl;
          cout << DikeUtil().Reset() << endl;
          
          dikeSQLParam.schema = cfg->getString("Configuration.Schema");
          dikeSQLParam.query = cfg->getString("Configuration.Query");
          dikeSQLParam.blockSize = cfg->getUInt64("Configuration.BlockSize");

          ostream& toClient = resp.send();
          toClient.flush();

          Poco::Net::HTTPServerRequestImpl & req_impl = (Poco::Net::HTTPServerRequestImpl &)req;          
          Poco::Net::StreamSocket toClientSocket = req_impl.detachSocket();
          
          dikeSQL.Run(&dikeSQLParam, (Poco::Net::HTTPSession*)&hdfs_session, &fromHDFS, &toClientSocket );       
          
        } catch (Poco::NotFoundException&)
        {
          cout << DikeUtil().Red() << "Exeption while parsing readParam" << endl;
          cout << DikeUtil().Reset() << endl;
        }      
      } else {
        ostream& toClient = resp.send();
        Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
      }              
      
      //hdfs_session.close();
      resp.write(cout);
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
    HTTPServerParams* nameNodeParams = new HTTPServerParams;
    HTTPServerParams* dataNodeParams = new HTTPServerParams;

    dataNodeParams->setKeepAlive(false);
    dataNodeParams->setMaxThreads(16);
    dataNodeParams->setMaxQueued(128);

    HTTPServer nameNode(new NameNodeHandlerFactory, ServerSocket(9860), nameNodeParams);
    HTTPServer dataNode(new DataNodeHandlerFactory, ServerSocket(9859), dataNodeParams);

    nameNode.start();
    dataNode.start();

#if _DEBUG
    cout << endl << "Server started (Debug)" << endl;
#else
    cout << endl << "Server started" << endl;
#endif

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