#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/HTTPClientSession.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLWriter.h>
#include <Poco/URI.h>

#include "Poco/StreamCopier.h"

#include <iostream>
#include <sstream>
#include <map>

#include "DikeUtil.hpp"
#include "S3Handlers.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;


void ProxyHandler::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{      
    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " S3 Proxy Start " << DikeUtil().Reset() << endl;      
    }
    
    std::istream& fromClient = req.stream();

    /* Instantiate a copy of original request */
    HTTPRequest s3_req((HTTPRequest)req);

    /* Redirect to S3 port */
    //s3_req.setHost(dikeConfig["dike.S3.endpoint.http-address"]);

    if(verbose) {      
      cout << s3_req.getURI() << endl;
      s3_req.write(cout);
    }

    /* Open S3 session */    
    SocketAddress s3SocketAddress = SocketAddress(dikeConfig["dike.S3.endpoint.http-address"]);
    HTTPClientSession s3Session(s3SocketAddress);
    std::ostream& toS3 = s3Session.sendRequest(s3_req);
    Poco::StreamCopier::copyStream(fromClient, toS3, 4096);  
    HTTPResponse s3_resp;
    std::istream& fromS3 = s3Session.receiveResponse(s3_resp);
    (HTTPResponse &)resp = s3_resp;    

    if(verbose) {
      resp.write(cout);
    }

    ostream& toClient = resp.send();
    Poco::StreamCopier::copyStream(fromS3, toClient, 4096);
    toClient.flush();

    if(verbose) {
      cout << DikeUtil().Yellow() << DikeUtil().Now() << " S3 Proxy End " << DikeUtil().Reset() << endl;
    }
}