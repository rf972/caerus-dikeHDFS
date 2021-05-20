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
#include "Poco/Net/HTTPHeaderStream.h"

#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>


#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <iostream>
#include <algorithm>
#include <map>

#include "S3Handlers.hpp"
#include "DikeUtil.hpp"
#include "DikeIO.hpp"
#include "DikeSQL.hpp"
#include "DikeStream.hpp"
#include "DikeHTTPRequestHandler.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

class NameNodeHandler : public DikeHTTPRequestHandler {
  public:  
  NameNodeHandler(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {    
    std::istream& fromClient = req.stream();
    if(verbose){
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN Start " << DikeUtil().Reset() << endl;      
    }
    
    /* Instantiate a copy of original request */
    HTTPRequest hdfs_req((HTTPRequest)req);

    /* Redirect to HDFS port */
    hdfs_req.setHost(dikeConfig["dfs.namenode.http-address"]);

    if(verbose) {      
      cout << hdfs_req.getURI() << endl;
      hdfs_req.write(cout);
    }

    /* Open HDFS session */    
    SocketAddress namenodeSocketAddress = SocketAddress(dikeConfig["dfs.namenode.http-address"]);
    HTTPClientSession session(namenodeSocketAddress);
    std::ostream& toHDFS = session.sendRequest(hdfs_req);
    Poco::StreamCopier::copyStream(fromClient, toHDFS, 8192);  
    HTTPResponse hdfs_resp;
    std::istream& fromHDFS = session.receiveResponse(hdfs_resp);
    (HTTPResponse &)resp = hdfs_resp;    
    
    if(req.has("ReadParam") && resp.has("Location")) {
      //cout << DikeUtil().Blue() << resp.get("Location") << DikeUtil().Reset() << endl;      
      Poco::URI uri = Poco::URI(resp.get("Location"));      
      uri.setPort(std::stoi(dikeConfig["dike.dfs.ndp.http-port"]));
      resp.set("Location", uri.toString());
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

class DataNodeHandler : public DikeHTTPRequestHandler {
public:
  DataNodeHandler(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {
    DikeSQLParam dikeSQLParam;

    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN Start " << DikeUtil().Reset() << endl;
    }

    HTTPRequest hdfs_req((HTTPRequest)req);
    string host = req.getHost();    
    host = host.substr(0, host.find(':'));
    hdfs_req.setHost(host, std::stoi(dikeConfig["dfs.datanode.http-port"]));
    
    if(verbose) {
      cout << hdfs_req.getURI() << endl;
    }
    
    Poco::URI uri = Poco::URI(hdfs_req.getURI());
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

    DikeHDFSSession hdfs_session(host, std::stoi(dikeConfig["dfs.datanode.http-port"]));
    hdfs_session.sendRequest(hdfs_req);

    HTTPResponse hdfs_resp;
    hdfs_session.readResponseHeader(hdfs_resp);

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
          cout << "Query: " << cfg->getString("Configuration.Query") << endl;
          cout << "BlockSize: " << cfg->getString("Configuration.BlockSize") << endl;
          cout << DikeUtil().Reset() << endl;
        }        
        
        dikeSQLParam.query = cfg->getString("Configuration.Query");
        dikeSQLParam.blockSize = cfg->getUInt64("Configuration.BlockSize");
        dikeSQLParam.headerInfo = cfg->getString("Configuration.HeaderInfo", "IGNORE");

        ostream& toClient = resp.send();
        toClient.flush();

        Poco::Net::HTTPServerRequestImpl & req_impl = (Poco::Net::HTTPServerRequestImpl &)req;          
        Poco::Net::StreamSocket toClientSocket = req_impl.detachSocket();
                
        DikeInSession input(&hdfs_session);
        DikeOut output(&toClientSocket);
        dikeSQL.Run(&dikeSQLParam, &input, &output);       
        
      } catch (Poco::NotFoundException&) {
        cout << DikeUtil().Red() << "Exeption while parsing readParam" << endl;
        cout << DikeUtil().Reset() << endl;
      }      
    } else {
      std::istream& fromHDFS = hdfs_session.receiveResponse(hdfs_resp);
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
  DikeConfig dikeConfig;
  NameNodeHandlerFactory(int verbose, DikeConfig& dikeConfig):HTTPRequestHandlerFactory() {
    this->verbose = verbose;
    this->dikeConfig = dikeConfig;
  }
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new NameNodeHandler(verbose, dikeConfig);
  }
};

class DataNodeHandlerFactory : public HTTPRequestHandlerFactory {
public:
  int verbose = 0;
  DikeConfig dikeConfig;
  DataNodeHandlerFactory(int verbose, DikeConfig& dikeConfig):HTTPRequestHandlerFactory() {
    this->verbose = verbose;
    this->dikeConfig = dikeConfig;
  }

  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    return new DataNodeHandler(verbose, dikeConfig);
  }
};

class S3GatewayHandlerFactory : public HTTPRequestHandlerFactory {
public:
  int verbose = 0;
  DikeConfig dikeConfig;
  S3GatewayHandlerFactory(int verbose, DikeConfig& dikeConfig):HTTPRequestHandlerFactory() {
    this->verbose = verbose;
    this->dikeConfig = dikeConfig;
  }

  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    Poco::URI uri = Poco::URI(req.getURI());
    if (uri.getQuery() == "select&select-type=2") {
      return new SelectObjectContent(verbose, dikeConfig);
    }
    
    
    if(uri.getQuery().find("list-type=2") == 0){
      return new ListObjectsV2(verbose, dikeConfig);
    }    

    DikeConfig::iterator it = dikeConfig.find("dike.S3.endpoint.http-address");
    if(it != dikeConfig.end()) { // S3 proxy is configured
      return new ProxyHandler(verbose, dikeConfig);
    }


    cout << DikeUtil().Yellow() << "Unsupported query " << uri.getQuery() << DikeUtil().Reset() << endl;
    return NULL;
  }
};

class DikeServerApp : public ServerApplication
{
  public:
  int verbose = 0;
  DikeConfig dikeConfig;

  protected:
  void defineOptions(OptionSet& options)
 	{
 		Application::defineOptions(options);
 
 		options.addOption(
 			Option("verbose", "v", "Verbose output")
 				.required(false)
 				.repeatable(false)
 				.callback(OptionCallback<DikeServerApp>(this, &DikeServerApp::handleVerbose)));
 	}

	void handleVerbose(const std::string& name, const std::string& value)
	{
    verbose = 1;
	}

  void loadDikeConfig()
  {
    std::string base = "dike";
 		AbstractConfiguration::Keys keys;
		config().keys(base, keys);
    for (AbstractConfiguration::Keys::const_iterator it = keys.begin(); it != keys.end(); ++it) {
      std::string name = config().getString(base + "." + (*it) + ".name");
      std::string value = config().getString(base + "." + (*it) + ".value");
      logger().information(name + " = " + value);
      dikeConfig[name] = value;
    }
  }

  int main(const vector<string> & argv)
  {
    HTTPServerParams* nameNodeParams = new HTTPServerParams;
    HTTPServerParams* dataNodeParams = new HTTPServerParams;
    HTTPServerParams* s3GatewayParams = new HTTPServerParams;

    Poco::Timespan timeout = Poco::Timespan(60*60*24, 0);

    nameNodeParams->setTimeout(timeout);
    dataNodeParams->setTimeout(timeout);
    s3GatewayParams->setTimeout(timeout);
 
    loadConfiguration(Poco::Util::Application::PRIO_DEFAULT);   
    loadDikeConfig();

    dataNodeParams->setKeepAlive(false);
    dataNodeParams->setMaxThreads(16);
    dataNodeParams->setMaxQueued(128);
        
    HTTPServer nameNode(new NameNodeHandlerFactory(verbose, dikeConfig),                         
                        ServerSocket(std::stoi(dikeConfig["dike.dfs.gateway.http-port"])),
                        nameNodeParams);

    HTTPServer dataNode(new DataNodeHandlerFactory(verbose, dikeConfig),
                        ServerSocket(std::stoi(dikeConfig["dike.dfs.ndp.http-port"])), 
                        dataNodeParams);

    HTTPServer s3Gateway(new S3GatewayHandlerFactory(verbose, dikeConfig),
                        ServerSocket(std::stoi(dikeConfig["dike.S3.gateway.http-port"])), 
                        s3GatewayParams);

    nameNode.start();
    dataNode.start();
    s3Gateway.start();

#if _DEBUG
    cout << endl << "Server started (Debug)" << endl;
#else
    cout << endl << "Server started" << endl;
#endif

    waitForTerminationRequest();  // wait for CTRL-C or kill

    cout << endl << "Shutting down..." << endl;
    nameNode.stop();
    dataNode.stop();
    s3Gateway.stop();

    return Application::EXIT_OK;
  }
};

int main(int argc, char** argv)
{  
  DikeServerApp app;
  
  return app.run(argc, argv);
}
