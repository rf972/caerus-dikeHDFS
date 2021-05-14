#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLWriter.h>

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
using namespace std;

#define FNAME_MAX 80
#define TSTAMP_MAX 40
void ListObjectsV2::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{
    stringstream xmlstream;
    Poco::XML::XMLString element;
    Poco::XML::XMLString value;
    Poco::XML::XMLWriter writer(xmlstream, 0);    

    writer.startDocument();
    
    writer.startElement("", "", XMLString("ListBucketResult"));
    writer.characters(XMLString("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
    
    writer.startElement("", "", XMLString("Name"));
    writer.characters("FileNameFromHDFS");    
    writer.endElement("", "", XMLString("Name"));

    writer.startElement("", "", XMLString("Prefix"));
    writer.characters("Prefix");
    writer.endElement("", "", XMLString("Prefix"));

    writer.startElement("", "", XMLString("KeyCount"));
    writer.characters(to_string(1));    
    writer.endElement("", "", XMLString("KeyCount"));

    writer.startElement("", "", XMLString("MaxKeys"));
    writer.characters("1024");    
    writer.endElement("", "", XMLString("MaxKeys"));

    writer.startElement("", "", XMLString("Delimiter"));        
    writer.endElement("", "", XMLString("Delimiter"));

    writer.startElement("", "", XMLString("IsTruncated"));
    writer.characters(XMLString("false"));    
    writer.endElement("", "", XMLString("IsTruncated"));

    // Loop starts here
    for(int i = 0; i < 1; i++) {
        writer.startElement("", "", XMLString("Contents"));
        
            writer.startElement("", "", XMLString("Key"));
            writer.characters(XMLString("FileKeyFromHDFS"));
            writer.endElement("", "", XMLString("Key"));

            writer.startElement("", "", XMLString("LastModified"));
            //writer.characters(XMLString(contents[i].lastModified));                
            writer.characters(XMLString("2020-10-27T17:44:12.056Z"));                
            writer.endElement("", "", XMLString("LastModified"));

            writer.startElement("", "", XMLString("ETag"));
            writer.characters(XMLString("&#34;00000000000000000000000000000000-1&#34;"));    
            writer.endElement("", "", XMLString("ETag"));

            writer.startElement("", "", XMLString("Size"));
            writer.characters(to_string(512<<20));    
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

    cout << DikeUtil().Yellow() << DikeUtil().Now() << " Done " << DikeUtil().Reset();
    cout << req.getURI() << endl;
}