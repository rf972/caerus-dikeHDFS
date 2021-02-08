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
#include "DikeIO.hpp"
#include "DikeSQL.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;


class DikeIn : public DikeIO {
  public:
  std::istream * inStream = NULL;

  DikeIn(std::istream * stream){
    inStream = stream;
  }

  ~DikeIn(){ }
  virtual int write(const char * buf, uint32_t size) {
    return -1;
  }
  virtual int read(char * buf, uint32_t size) {
    if(inStream) {
      int n = 0;
      int len = size;
      try {
        while( len > 0 && inStream->good() ) {
            inStream->read((char*)&buf[n], len );
            int i = inStream->gcount();
            n += i;
            len -= i;
            if(i <= 0){
                return n;
            }
        }
      } catch (...) {
        std::cout << "DikeIn: Caught exception " << std::endl;
      }     
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
        std::cout << "DikeIn: Caught exception " << std::endl;
      }
    }
    return -1;
  }
};

class NameNodeHandler : public Poco::Net::HTTPRequestHandler {
public:  
  int verbose = 0;
    NameNodeHandler(int verbose): HTTPRequestHandler() {
      this->verbose = verbose;
  }

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {    
    std::istream& fromClient = req.stream();
    if(verbose){
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN Start " << DikeUtil().Reset() << endl;      
    }
    
    /* Instantiate a copy of original request */
    HTTPRequest hdfs_req((HTTPRequest)req);

    /* Redirect to HDFS port */
    string host = req.getHost();    
    host = host.substr(0, host.find(':'));
    hdfs_req.setHost(host, 9870);

    if(verbose) {
      cout << hdfs_req.getURI() << endl;     
      hdfs_req.write(cout);
    }

    /* Open HDFS session */
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

    if(verbose) {
      resp.write(cout);
    }
    ostream& toClient = resp.send();
    Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
    toClient.flush();

    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN End " << DikeUtil().Reset() << endl;
    }
  }   
};

class DataNodeHandler : public HTTPRequestHandler {
public:  
  int verbose = 0;
  DataNodeHandler(int verbose): HTTPRequestHandler() {
    this->verbose = verbose;
  }

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {
    DikeSQLParam dikeSQLParam;

    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN Start " << DikeUtil().Reset() << endl;
    }

    HTTPRequest hdfs_req((HTTPRequest)req);

    string host = req.getHost();    
    host = host.substr(0, host.find(':'));
    hdfs_req.setHost(host, 9864);

    if(verbose) {
      cout << hdfs_req.getURI() << endl;
    }
    Poco::URI uri(hdfs_req.getURI());
    Poco::URI::QueryParameters uriParams = uri.getQueryParameters();
    dikeSQLParam.blockOffset = 0;
    for(int i = 0; i < uriParams.size(); i++){        
      if(uriParams[i].first == "offset"){
        if(verbose) {
          cout << uriParams[i].first << ": " << uriParams[i].second << endl;
        }
        dikeSQLParam.blockOffset = std::stoull(uriParams[i].second);
      }
    }

    if(verbose) {
      hdfs_req.write(cout);
    }

    HTTPClientSession hdfs_session(host, 9864);
    hdfs_session.sendRequest(hdfs_req);
    HTTPResponse hdfs_resp;

    std::istream& fromHDFS = hdfs_session.receiveResponse(hdfs_resp);
    (HTTPResponse &)resp = hdfs_resp;
    //cout << "From HDFS we got \"" << hdfs_resp.getTransferEncoding() << "\" TransferEncoding" << endl;

    if(req.has("ReadParam")) {
      resp.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);
      resp.setChunkedTransferEncoding(false);    
      resp.setKeepAlive(false);

      string readParam = req.get("ReadParam");
      if(verbose) {
        cout << DikeUtil().Blue();
      }
      DikeSQL dikeSQL;

      try{
        std::istringstream readParamStream(readParam.c_str());      
        std::istream& xmlStream(readParamStream);
        AbstractConfiguration *cfg = new XMLConfiguration(xmlStream);
        if(verbose) {
          cout << "Name: " << cfg->getString("Name") << endl;
          cout << "Schema: " << cfg->getString("Configuration.Schema") << endl;
          cout << "Query: " << cfg->getString("Configuration.Query") << endl;
          cout << "BlockSize: " << cfg->getString("Configuration.BlockSize") << endl;
          cout << DikeUtil().Reset() << endl;
        }        
        dikeSQLParam.schema = cfg->getString("Configuration.Schema");
        dikeSQLParam.query = cfg->getString("Configuration.Query");
        dikeSQLParam.blockSize = cfg->getUInt64("Configuration.BlockSize");

        ostream& toClient = resp.send();
        toClient.flush();

        Poco::Net::HTTPServerRequestImpl & req_impl = (Poco::Net::HTTPServerRequestImpl &)req;          
        Poco::Net::StreamSocket toClientSocket = req_impl.detachSocket();
        
        DikeIn input(&fromHDFS);
        DikeOut output(&toClientSocket);
        dikeSQL.Run(&dikeSQLParam, &input, &output);       
        
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
    if(verbose) {
      resp.write(cout);
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN End " << DikeUtil().Reset() << endl;
    }
  }   
};

class NameNodeHandlerFactory : public HTTPRequestHandlerFactory {
public:
  int verbose = 0;
  NameNodeHandlerFactory(const vector<string> & argv):HTTPRequestHandlerFactory() {
    for (auto arg : argv) {
      if(arg == "-v"){
        verbose = 1;
      }
    }
  }
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new NameNodeHandler(verbose);
  }
};

class DataNodeHandlerFactory : public HTTPRequestHandlerFactory {
public:
  int verbose = 0;
  DataNodeHandlerFactory(const vector<string> & argv):HTTPRequestHandlerFactory() {
    for (auto arg : argv) {
      if(arg == "-v"){
        verbose = 1;
      }
    }
  }
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new DataNodeHandler(verbose);
  }
};

class DikeServerApp : public ServerApplication
{
protected:
  int main(const vector<string> & argv)
  {
    HTTPServerParams* nameNodeParams = new HTTPServerParams;
    HTTPServerParams* dataNodeParams = new HTTPServerParams;

    dataNodeParams->setKeepAlive(false);
    dataNodeParams->setMaxThreads(16);
    dataNodeParams->setMaxQueued(128);

    HTTPServer nameNode(new NameNodeHandlerFactory(argv), ServerSocket(9860), nameNodeParams);
    HTTPServer dataNode(new DataNodeHandlerFactory(argv), ServerSocket(9859), dataNodeParams);

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