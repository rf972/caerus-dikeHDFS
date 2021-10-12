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

#include <Poco/AutoPtr.h>

#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <iostream>
#include <algorithm>
#include <map>
#include <mutex>
#include <sstream>
#include <omp.h>

//#include "S3Handlers.hpp"
#include "DikeUtil.hpp"
#include "DikeIO.hpp"
#include "DikeSQL.hpp"
#include "DikeStream.hpp"
#include "DikeHTTPRequestHandler.hpp"

#include "dikeLambda/TpchQ1.hpp"
#include "dikeLambda/LambdaProcessor.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

static int dikeNodeType = 0;
enum {
    COMPUTE_NODE = 0,
    STORAGE_NODE = 1,
};

static std::atomic<int> dataNodeReqCount(0);
static int dikeStorageMaxRequests = 2;

class NameNodeHandler : public DikeHTTPRequestHandler {
  public:
  static std::mutex lock;
  static std::map<std::string, HTTPResponse> responseMap;

  NameNodeHandler(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {    
    std::istream& fromClient = req.stream();
    if(verbose){
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN Start " << DikeUtil().Reset() << endl;      
    }

    std::string reqUri = req.getURI();
    //cout << "URI " << uri << endl;

    lock.lock();
    if(req.has("ReadParam") && responseMap.count(reqUri)) { // We have cached response
        (HTTPResponse &)resp = responseMap[reqUri];
        lock.unlock();
        if(verbose) {
            cout << "Use cached responce " << reqUri << endl;
        }
    } else {
        lock.unlock();
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
            if(dikeNodeType == STORAGE_NODE ) { // Np caching on NCP
                if(verbose) {
                    cout << "Cache HDFS responce at " << reqUri << endl;
                }
                lock.lock();
                (HTTPResponse &)responseMap[reqUri] = resp;
                lock.unlock();
            }
        } else { // Not pushdowns request
            ostream& toClient = resp.send();
            Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
            toClient.flush();
            return;
        }
    }
    
    if(req.has("ReadParam") && resp.has("Location")) {        
        //cout << DikeUtil().Blue() << resp.get("Location") << DikeUtil().Reset() << endl;      
        Poco::URI uri = Poco::URI(resp.get("Location"));
        int ndpPort = std::stoi(dikeConfig["dike.dfs.ndp.http-port"]);
        string host = req.getHost();
        host = host.substr(0, host.find(':'));
        // On a storage node perform NDP if we have compute capacity
        if(dikeNodeType == STORAGE_NODE && dataNodeReqCount < dikeStorageMaxRequests) {
            uri.setHost(host); // Client should be redirected back to our address
            uri.setPort(ndpPort);
            resp.set("Location", uri.toString());
        }

        // On a compute node perform NDP if storage is not doing it
        if(dikeNodeType == COMPUTE_NODE && ndpPort != uri.getPort()) {
            uri.setHost(host); // Client should be redirected back to our address
            uri.setPort(ndpPort);
            resp.set("Location", uri.toString());            
        }
      
      //cout << DikeUtil().Blue() << resp.get("Location") << DikeUtil().Reset() << endl;
    }

    if(verbose) {
      resp.write(cout);    
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " NN End " << DikeUtil().Reset() << endl;
    }

    ostream& toClient = resp.send();
    //Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
    toClient.flush();
  }   
};

class DataNodeHandler : public DikeHTTPRequestHandler {
public:
  DataNodeHandler(int verbose, DikeConfig & dikeConfig): DikeHTTPRequestHandler(verbose, dikeConfig){}

  virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
  {
    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN Start " << DikeUtil().Reset() << endl;
    }

    if(!req.has("ReadParam")) {
        HTTPRequest hdfs_req((HTTPRequest)req);
        string host = req.getHost();    
        host = host.substr(0, host.find(':'));
        hdfs_req.setHost(host, std::stoi(dikeConfig["dfs.datanode.http-port"]));
    
        if(verbose) {
            cout << hdfs_req.getURI() << endl;
        }
    
        //DikeHDFSSession hdfs_session(host, std::stoi(dikeConfig["dfs.datanode.http-port"]));
        HTTPClientSession  hdfs_session(host, std::stoi(dikeConfig["dfs.datanode.http-port"]));
        hdfs_session.sendRequest(hdfs_req);
        HTTPResponse hdfs_resp;
        //hdfs_session.readResponseHeader(hdfs_resp);
        
        std::istream& fromHDFS = hdfs_session.receiveResponse(hdfs_resp);
        (HTTPResponse &)resp = hdfs_resp;
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        resp.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);
        resp.setContentType("application/octet-stream");
        resp.setChunkedTransferEncoding(false);    
        resp.setKeepAlive(false);
        resp.set("Access-Control-Allow-Methods", "GET");
        resp.set("Access-Control-Allow-Origin", "*");

        ostream& toClient = resp.send();
        Poco::StreamCopier::copyStream(fromHDFS, toClient, 8192);
        if(verbose) {      
            resp.write(cout);      
            cout << DikeUtil().Yellow() << DikeUtil().Now() << " DN End " << DikeUtil().Reset() << endl;
        }
        return;
    }

    dataNodeReqCount += 1;
    if(req.has("ReadParam")) {
        resp.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        resp.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);
        resp.setContentType("application/octet-stream");
        resp.setChunkedTransferEncoding(false);    
        resp.setKeepAlive(false);
        resp.set("Access-Control-Allow-Methods", "GET");
        resp.set("Access-Control-Allow-Origin", "*");

        string readParam = req.get("ReadParam");
        if(verbose) {
            cout << DikeUtil().Blue();
        }
        
        DikeProcessor * dikeProcessor = NULL;
        DikeProcessorConfig dikeSQLConfig;

        std::istringstream readParamStream(readParam.c_str());      
        std::istream& xmlStream(readParamStream);        
        Poco::AutoPtr<AbstractConfiguration> cfg(new XMLConfiguration(xmlStream));

        try {
            std::stringstream ss;
            req.write(ss);
            
            dikeSQLConfig["system.verbose"] = std::to_string(verbose);

            dikeSQLConfig["Name"] = cfg->getString("Name");
            if(verbose) {
                cout << "Name = " + dikeSQLConfig["Name"] << endl;
            }

            if(dikeSQLConfig["Name"].compare("TpchQ1") == 0){
                dikeProcessor = (DikeProcessor *) new TpchQ1;
            } else if (dikeSQLConfig["Name"].compare("Lambda") == 0) {
                dikeProcessor = (DikeProcessor *) new LambdaProcessor;
            } else {
                dikeProcessor = (DikeProcessor *) new DikeSQL;
            }

            dikeSQLConfig["Request"] = ss.str();
            dikeSQLConfig["dfs.datanode.http-port"] = dikeConfig["dfs.datanode.http-port"];
            
            if(dikeConfig.count("dike.node.type") > 0) {
                dikeSQLConfig["dike.node.type"] = dikeConfig["dike.node.type"];
            }

            Poco::URI uri = Poco::URI(req.getURI());
            Poco::URI::QueryParameters uriParams = uri.getQueryParameters();
            dikeSQLConfig["BlockOffset"] = "0";
            for(int i = 0; i < uriParams.size(); i++){        
                if(uriParams[i].first == "offset"){
                    //dikeSQLParam.blockOffset = std::stoull(uriParams[i].second);
                    dikeSQLConfig["BlockOffset"] = uriParams[i].second;
                }
            }
            std::string base = "Configuration";
            AbstractConfiguration::Keys keys;
            cfg->keys(base, keys);
            for (AbstractConfiguration::Keys::const_iterator it = keys.begin(); it != keys.end(); ++it) {
                std::string name = base + "." + (*it);
                std::string value = cfg->getString(name);        
                dikeSQLConfig[name] = value;
                if(verbose) {
                    cout << name + " = " + value << endl;
                }
            }
            
            if(verbose) {
                cout << DikeUtil().Reset() << endl;
            }

            ostream& toClient = resp.send();
            toClient.flush();
#if 1
            Poco::Net::HTTPServerRequestImpl & req_impl = (Poco::Net::HTTPServerRequestImpl &)req;          
            Poco::Net::StreamSocket toClientSocket = req_impl.detachSocket();                                
            DikeOut output(&toClientSocket);
#else
            DikeOut output(&toClient);
#endif
            
            dikeProcessor->Run(dikeSQLConfig, &output);       
        } catch (Poco::NotFoundException&) {
            cout << DikeUtil().Red() << "Exeption while parsing readParam" << endl;
            cout << DikeUtil().Reset() << endl;
        }
        if(dikeProcessor) {
            delete dikeProcessor;
        }
    }    
    
    dataNodeReqCount -= 1;
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

#if 0
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
#endif

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

    if(dikeConfig.count("dike.node.type") > 0) {
        dikeNodeType = std::stoi(dikeConfig["dike.node.type"]);
        //std::cout << "dikeNodeType " << dikeNodeType << std::endl;
    }

    if(dikeConfig.count("dike.storage.max.requests") > 0) {
        dikeStorageMaxRequests = std::stoi(dikeConfig["dike.storage.max.requests"]);
        //std::cout << "dikeStorageMaxRequests " << dikeStorageMaxRequests << std::endl;
    }
    
  }

  int main(const vector<string> & argv)
  {
    HTTPServerParams* nameNodeParams = new HTTPServerParams;
    HTTPServerParams* dataNodeParams = new HTTPServerParams;
    //HTTPServerParams* s3GatewayParams = new HTTPServerParams;

    Poco::Timespan timeout = Poco::Timespan(60*60*24, 0);

    nameNodeParams->setTimeout(timeout);
    dataNodeParams->setTimeout(timeout);
    //s3GatewayParams->setTimeout(timeout);
 
    loadConfiguration(Poco::Util::Application::PRIO_DEFAULT);   
    loadDikeConfig();

    dataNodeParams->setKeepAlive(false);
    dataNodeParams->setMaxThreads(32);
    dataNodeParams->setMaxQueued(128);
        
    HTTPServer nameNode(new NameNodeHandlerFactory(verbose, dikeConfig),                         
                        ServerSocket(std::stoi(dikeConfig["dike.dfs.gateway.http-port"])),
                        nameNodeParams);

    HTTPServer dataNode(new DataNodeHandlerFactory(verbose, dikeConfig),
                        ServerSocket(std::stoi(dikeConfig["dike.dfs.ndp.http-port"])), 
                        dataNodeParams);
#if 0
    HTTPServer s3Gateway(new S3GatewayHandlerFactory(verbose, dikeConfig),
                        ServerSocket(std::stoi(dikeConfig["dike.S3.gateway.http-port"])), 
                        s3GatewayParams);
#endif

    nameNode.start();
    dataNode.start();
    //s3Gateway.start();

#if _DEBUG
    cout << endl << "Server started (Debug)" << endl;
#else
    cout << endl << "Server started" << endl;
#endif

    waitForTerminationRequest();  // wait for CTRL-C or kill

    cout << endl << "Shutting down..." << endl;
    nameNode.stop();
    dataNode.stop();
    //s3Gateway.stop();

    return Application::EXIT_OK;
  }
};

int main(int argc, char** argv)
{  
    DikeServerApp app;
  
    #pragma omp parallel
    {
        #pragma omp single
        std::cout << "omp_get_num_threads " << omp_get_num_threads() << std::endl;
    }
   
    return app.run(argc, argv);
}

std::mutex NameNodeHandler::lock;
std::map<std::string, HTTPResponse> NameNodeHandler::responseMap;

