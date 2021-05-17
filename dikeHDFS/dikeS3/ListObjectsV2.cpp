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

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include "Poco/StreamCopier.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTime.h"
#include "Poco/LocalDateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"

#include <iostream>
#include <sstream>
#include <map>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "DikeUtil.hpp"
#include "S3Handlers.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace Poco::XML;
using namespace Poco::JSON;
using namespace std;

#define FNAME_MAX 80
#define TSTAMP_MAX 40

string JsonGetValue(Poco::JSON::Object::Ptr jsonObject, string & key) 
{    
    Poco::Dynamic::Var var = jsonObject->get(key);
    return var.convert<std::string>();    
}

void ListObjectsV2::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{
    stringstream xmlstream;
    Poco::XML::XMLString element;
    Poco::XML::XMLString value;
    Poco::XML::XMLWriter writer(xmlstream, 0);    

    if(verbose) {
        req.write(cout);
    }

    Poco::URI uriS3 = Poco::URI(req.getURI());
    using QueryParameters = std::vector<std::pair<std::string, std::string>>;
    using ListObjectsV2Parameters = std::map<std::string, std::string>;

    ListObjectsV2Parameters param;
    param["prefix"] = ""; // We need to preseed the prefix
    QueryParameters queryParametersS3 = uriS3.getQueryParameters();
    for (auto & it : queryParametersS3) {
        param[it.first] = it.second;
        //cout << it.first << " = " << it.second << endl;
    }
    param["fileName"] = uriS3.getPath();

    string authorization = req.get("Authorization");
    std::size_t startPos = authorization.find("Credential=");
    startPos = authorization.find("=", startPos) + 1;
    std::size_t endPos = authorization.find("/", startPos);
    string userName = authorization.substr(startPos, endPos - startPos);    
    param["userName"]  = userName;

    using Pair = std::pair<std::string, std::string>;

    HTTPRequest nameNodeReq;
    nameNodeReq.setHost(dikeConfig["dfs.namenode.http-address"]);    
    Poco::URI uriHDFS = Poco::URI("/webhdfs/v1" + param["fileName"]);
    QueryParameters queryParametersHDFS;
    queryParametersHDFS.push_back(Pair("op","LISTSTATUS"));
    queryParametersHDFS.push_back(Pair("user.name",param["userName"]));

    uriHDFS.setQueryParameters(queryParametersHDFS);
    //cout << uriHDFS.toString() << endl;
    nameNodeReq.setURI(uriHDFS.toString());
    
    /* Open HDFS Nane Node session */    
    SocketAddress nameNodeSocketAddress = SocketAddress(dikeConfig["dfs.namenode.http-address"]);
    HTTPClientSession nameNodeSession(nameNodeSocketAddress);
    HTTPResponse nameNodeResp;
    nameNodeSession.sendRequest(nameNodeReq);
    std::istream& fromHDFS =nameNodeSession.receiveResponse(nameNodeResp);
    string jsonString;
    Poco::StreamCopier::copyToString(fromHDFS, jsonString);

    //cout << jsonString << endl;
    Poco::JSON::Parser parser;    
    Poco::Dynamic::Var parserResult = parser.parse(jsonString);
    Poco::JSON::Object::Ptr json = parserResult.extract<Poco::JSON::Object::Ptr>();
    Poco::JSON::Object::Ptr fileStatuses = json->getObject("FileStatuses");

    Array::Ptr fileStatus = fileStatuses->getArray("FileStatus");
    //cout << "fileStatus array size : " << fileStatus->size() << endl;

    writer.startDocument();
    
    writer.startElement("", "", XMLString("ListBucketResult"));
    writer.characters(XMLString("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
    
    writer.startElement("", "", XMLString("Name"));
    writer.characters(XMLString(param["fileName"]));
    writer.endElement("", "", XMLString("Name"));

    writer.startElement("", "", XMLString("Prefix"));
    writer.characters(XMLString(param["prefix"]));
    writer.endElement("", "", XMLString("Prefix"));

    writer.startElement("", "", XMLString("KeyCount"));
    writer.characters(XMLString(to_string(fileStatus->size())));    
    writer.endElement("", "", XMLString("KeyCount"));

    writer.startElement("", "", XMLString("MaxKeys"));
    writer.characters(XMLString(param["max-keys"]));    
    writer.endElement("", "", XMLString("MaxKeys"));

    writer.startElement("", "", XMLString("Delimiter"));        
    writer.endElement("", "", XMLString("Delimiter"));

    writer.startElement("", "", XMLString("IsTruncated"));
    writer.characters(XMLString("false"));    
    writer.endElement("", "", XMLString("IsTruncated"));

    // Loop starts here
    for(int i = 0; i < fileStatus->size(); i++) {
        string type = fileStatus->getObject(i)->get("type").convert<std::string>();
        if( type.find("FILE") != 0) {
            continue;
        }
        string key = fileStatus->getObject(i)->get("pathSuffix").convert<std::string>();
        if(key.find(param["prefix"]) != 0) {
            continue;
        }
        Poco::Timestamp ts = std::stoull(fileStatus->getObject(i)->get("modificationTime").convert<std::string>()) * 1000;
        Poco::LocalDateTime ldt = Poco::LocalDateTime(Poco::DateTime(ts));

        writer.startElement("", "", XMLString("Contents"));
        
            writer.startElement("", "", XMLString("Key"));
            writer.characters(fileStatus->getObject(i)->get("pathSuffix").convert<std::string>());
            writer.endElement("", "", XMLString("Key"));

            writer.startElement("", "", XMLString("LastModified"));
            writer.characters(XMLString(Poco::DateTimeFormatter::format(ldt,"%Y-%n-%fT%H:%M:%S.%iZ")));                
            //writer.characters(XMLString("2020-10-27T17:44:12.056Z"));                
            writer.endElement("", "", XMLString("LastModified"));

            writer.startElement("", "", XMLString("ETag"));
            writer.characters(XMLString("&#34;00000000000000000000000000000000-1&#34;"));    
            writer.endElement("", "", XMLString("ETag"));

            writer.startElement("", "", XMLString("Size"));
            writer.characters(fileStatus->getObject(i)->get("length").convert<std::string>());
            writer.endElement("", "", XMLString("Size"));

            writer.startElement("", "", XMLString("StorageClass"));
            writer.characters(XMLString("STANDARD"));    
            writer.endElement("", "", XMLString("StorageClass"));

        writer.endElement("", "", XMLString("Contents"));
    }

    writer.endElement("", "", XMLString("ListBucketResult"));

    writer.endDocument();
    //cout << xmlstream.str() << endl;

    resp.setStatus(HTTPResponse::HTTP_OK);    
    resp.setContentType("application/xml");    

    ostream& outStream = resp.send();
    outStream.write(xmlstream.str().c_str(), xmlstream.str().length());
    outStream.flush();

    if(verbose){
        cout << DikeUtil().Yellow() << DikeUtil().Now() << " Done " << DikeUtil().Reset();
        cout << req.getURI() << endl;
    }
}