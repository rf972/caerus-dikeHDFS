#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPClientSession.h>

#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/URI.h>

/* XML related headers */
#include "Poco/DOM/DOMParser.h"
#include "Poco/DOM/DOMWriter.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/XML/XMLWriter.h"
#include <sstream>

#include <aws/event-stream/event_stream.h>
#include <aws/common/encoding.h>
#include <aws/checksums/crc.h> 

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdlib.h>
#include <ctype.h>
#include <memory.h>
#include <assert.h>
#include <stdio.h>

#include "S3Handlers.hpp"
#include "DikeUtil.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

const char MESSAGE_TYPE_HEADER[] = ":message-type";
const char MESSAGE_TYPE_EVENT[] = "event";

const char CONTENT_TYPE_HEADER[] = ":content-type";
const char CONTENT_TYPE_OCTET_STREAM[] = "application/octet-stream";

const char EVENT_TYPE_HEADER[] = ":event-type";
const char EVENT_TYPE_RECORDS[] = "Records";
const char EVENT_TYPE_END[] = "End";
const char EVENT_TYPE_CONT[] = "Cont";

const char ERROR_CODE_HEADER[] = ":error-code";
const char ERROR_MESSAGE_HEADER[] = ":error-message";
const char EXCEPTION_TYPE_HEADER[] = ":exception-type";

class DikeByteBuffer
{
  const int DIKE_BYTE_BUFFER_SIZE = 127<<10; 
  
  public:
    DikeByteBuffer() 
    {      
      struct aws_array_list headers;
      struct aws_allocator *alloc = aws_default_allocator();
      struct aws_event_stream_message msg;
      
      aws_event_stream_headers_list_init(&headers, alloc);        
      aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);
      aws_event_stream_add_string_header(&headers, CONTENT_TYPE_HEADER, sizeof(CONTENT_TYPE_HEADER) - 1, CONTENT_TYPE_OCTET_STREAM, sizeof(CONTENT_TYPE_OCTET_STREAM) - 1, 0);
      aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_RECORDS, sizeof(EVENT_TYPE_RECORDS) - 1, 0);  
      
      aws_event_stream_message_init(&msg, alloc, &headers, NULL);

      m_MsgLen = aws_event_stream_message_total_length(&msg);
      m_Msg.alloc = NULL;      
      m_Msg.message_buffer = (uint8_t *)aligned_alloc(128, m_MsgLen + DIKE_BYTE_BUFFER_SIZE);
      m_Msg.owns_buffer = 1;

      memcpy(m_Msg.message_buffer, aws_event_stream_message_buffer(&msg), m_MsgLen);
      aws_event_stream_message_clean_up(&msg);
      aws_event_stream_headers_list_cleanup(&headers);

      m_Payload = ( uint8_t *)aws_event_stream_message_payload(&m_Msg);
      m_Pos = 0;
      m_TotalBytes = 0;
    }

    ~DikeByteBuffer() {
        free(m_Msg.message_buffer);      
    }

    void Write(ostream& toClient, const char * buf, int len) {
        if(m_Pos + len > DIKE_BYTE_BUFFER_SIZE){        
            UpdateCrc(&m_Msg, m_Pos);
            toClient.write((const char *)(m_Msg.message_buffer) , m_MsgLen + m_Pos);
            m_TotalBytes += m_Pos;
            m_Pos = 0;
        }
        memcpy(m_Payload + m_Pos, buf, len); // This cost 0.8 sec
        m_Pos += len;
    }

    void copyStreamToAWS(std::istream& istr, ostream& toClient) {
        std::streamsize n;
        do {
            istr.read((char*)(m_Payload + m_Pos), DIKE_BYTE_BUFFER_SIZE - m_Pos);
            n = istr.gcount();
            if (n > 0) {
                m_Pos += n;
                UpdateCrc(&m_Msg, m_Pos);
                toClient.write((const char *)(m_Msg.message_buffer) , m_MsgLen + m_Pos);
                m_TotalBytes += m_Pos;
                m_Pos = 0;
            }
        } while (n > 0);
    }

    void Flush(ostream& toClient) {      
        UpdateCrc(&m_Msg, m_Pos);      
        toClient.write((const char *)(m_Msg.message_buffer), m_MsgLen + m_Pos);
        m_TotalBytes += m_Pos;
        m_Pos = 0;
    }

    void UpdateCrc( struct aws_event_stream_message *message, uint32_t payload_len) {
        uint32_t total_length = 0;
        uint32_t headers_length = aws_event_stream_message_headers_len(message);    
        
        total_length =
            (uint32_t)(AWS_EVENT_STREAM_PRELUDE_LENGTH + headers_length + payload_len + AWS_EVENT_STREAM_TRAILER_LENGTH);
            
        aws_write_u32(total_length, message->message_buffer);
        uint8_t *buffer_offset = message->message_buffer + sizeof(total_length);
        aws_write_u32(headers_length, buffer_offset);   
        buffer_offset += sizeof(headers_length);
    
        uint32_t running_crc = aws_checksums_crc32(message->message_buffer, (int)(buffer_offset - message->message_buffer), 0);

        const uint8_t *message_crc_boundary_start = buffer_offset;
        aws_write_u32(running_crc, buffer_offset);

        buffer_offset = message->message_buffer + AWS_EVENT_STREAM_PRELUDE_LENGTH + headers_length;    
        buffer_offset += payload_len;

        running_crc = aws_checksums_crc32(
            message_crc_boundary_start, (int)(buffer_offset - message_crc_boundary_start), running_crc);
        aws_write_u32(running_crc, buffer_offset);        
    }

    int SendEnd(ostream& toClient) {
        struct aws_array_list headers;
        struct aws_allocator *alloc = aws_default_allocator();
        struct aws_event_stream_message msg;        
        
        aws_event_stream_headers_list_init(&headers, alloc);        
        aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);    
        aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_END, sizeof(EVENT_TYPE_END) - 1, 0);
        aws_event_stream_message_init(&msg, alloc, &headers, NULL);    
        toClient.write((const char *)aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));    
        aws_event_stream_message_clean_up(&msg);
        aws_event_stream_headers_list_cleanup(&headers);

        return 0;
    }    

    int SendCont(ostream& toClient) {  
        struct aws_array_list headers;
        struct aws_allocator *alloc = aws_default_allocator();
        struct aws_event_stream_message msg;        
        
        aws_event_stream_headers_list_init(&headers, alloc);        
        aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);    
        aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_CONT, sizeof(EVENT_TYPE_CONT) - 1, 0);
        aws_event_stream_message_init(&msg, alloc, &headers, NULL);    
        toClient.write((const char *)aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));    
        aws_event_stream_message_clean_up(&msg);
        aws_event_stream_headers_list_cleanup(&headers);

        return 0;
    }

  public:
    int m_TotalBytes;

  private:    
    int m_Pos;
    struct aws_event_stream_message m_Msg;
    uint8_t *m_Payload;
    int m_MsgLen;
};

void AbstractConfigutationWrite(AbstractConfiguration & cfg, const std::string& base, std::ostream& ostr)
{
    AbstractConfiguration::Keys keys;
    cfg.keys(base, keys);
    if (keys.empty()) {
        if (cfg.hasProperty(base)) {
            std::string msg;
            msg.append(base);
            msg.append(" = ");
            msg.append(cfg.getString(base));
            ostr << msg << endl;
        }
    } else {
        for (AbstractConfiguration::Keys::const_iterator it = keys.begin(); it != keys.end(); ++it) {
            std::string fullKey = base;
            if (!fullKey.empty()) fullKey += '.';
            fullKey.append(*it);
            AbstractConfigutationWrite(cfg, fullKey, ostr);
        }
    }
}

void SelectObjectContent::handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
{
    resp.setStatus(HTTPResponse::HTTP_OK);
    resp.set("Content-Security-Policy", "block-all-mixed-content");
    resp.set("Vary", "Origin");
    resp.set("X-Amz-Request-Id", "1640125B8EDA3957");
    resp.set("X-Xss-Protection", "1; mode=block");

    resp.setContentType("application/octet-stream");    
    resp.setChunkedTransferEncoding(true);    
    resp.setKeepAlive(true);

    if(verbose) {
        req.write(cout);
    }

    ostream& toClient = resp.send();    
    Poco::URI uri = Poco::URI(req.getURI());
    
    //cout << "uri.getQuery() : " << uri.getQuery() << endl;
    //cout << "uri.getPath() : " << uri.getPath() << endl;
    //cout << "Authorization : " << req.get("Authorization") << endl;

    string authorization = req.get("Authorization");
    std::size_t startPos = authorization.find("Credential=");
    startPos = authorization.find("=", startPos) + 1;
    std::size_t endPos = authorization.find("/", startPos);
    string userName = authorization.substr(startPos, endPos - startPos);
    //cout << "User name: " << userName << endl;
    
    AbstractConfiguration *cfg = new XMLConfiguration(req.stream());
    //AbstractConfigutationWrite(*cfg, "", cout);

    string fileName = uri.getPath();
    //cout << "File name: " << fileName << endl;
    string sqlQuery = cfg->getString("Expression");
    //cout << "SQL " << sqlQuery << endl;

    std::map<std::string, std::string> readParam;
    readParam["userName"] = userName;
    readParam["fileName"] = fileName;
    readParam["sqlQuery"] = sqlQuery;
    readParam["ScanRange.Start"] = cfg->getString("ScanRange.Start", "0");
    readParam["ScanRange.End"] = cfg->getString("ScanRange.End", "0");

    readFromHdfs(readParam, toClient);
}

void SelectObjectContent::readFromHdfs(std::map<std::string, std::string> readParam, std::ostream & toClient)
{    
    using QueryParameters = std::vector<std::pair<std::string, std::string>>;
    using Pair = std::pair<std::string, std::string>;

    HTTPRequest nameNodeReq;
    nameNodeReq.setHost(dikeConfig["dfs.namenode.http-address"]);
    /* We need to create URI similar to this: /webhdfs/v1/tpch-test/lineitem.csv?op=OPEN&user.name=peter&buffersize=131072 */
    Poco::URI uri = Poco::URI("/webhdfs/v1" + readParam["fileName"]);
    QueryParameters queryParameters;
    queryParameters.push_back(Pair("op","OPEN"));
    queryParameters.push_back(Pair("user.name",readParam["userName"]));
    queryParameters.push_back(Pair("buffersize","131072"));

    uri.setQueryParameters(queryParameters);
    //cout << uri.toString() << endl;
    nameNodeReq.setURI(uri.toString());
    
    /* Open HDFS Nane Node session */    
    SocketAddress nameNodeSocketAddress = SocketAddress(dikeConfig["dfs.namenode.http-address"]);
    HTTPClientSession nameNodeSession(nameNodeSocketAddress);
    HTTPResponse nameNodeResp;
    nameNodeSession.sendRequest(nameNodeReq);
    nameNodeSession.receiveResponse(nameNodeResp);
    
    uint64_t offset = std::stoull(readParam["ScanRange.Start"]);
    uint64_t blockSize = std::stoull(readParam["ScanRange.End"]) - offset;    

    /* Create XML string for NDP ReadParam */
    Poco::XML::Document* doc = new Poco::XML::Document();
    Poco::XML::Element* processorElement = doc->createElement("Processor");
    doc->appendChild(processorElement);
    Poco::XML::Element* nameElement = doc->createElement("Name");
    processorElement->appendChild(nameElement);
    Poco::XML::Text * nameText = doc->createTextNode("dikeSQL");        
    nameElement->appendChild((Poco::XML::Node *)nameText);

    Poco::XML::Element* configurationElement = doc->createElement("Configuration");
    processorElement->appendChild(configurationElement);
    Poco::XML::Element* queryElement = doc->createElement("Query");
    configurationElement->appendChild(queryElement);
    Poco::XML::CDATASection * queryCDATASection = doc->createCDATASection(readParam["sqlQuery"]);
    queryElement->appendChild((Poco::XML::Node *)queryCDATASection);
    
    Poco::XML::Element* blockSizeElement = doc->createElement("BlockSize");
    configurationElement->appendChild(blockSizeElement);
    Poco::XML::Text *  blockSizeText = doc->createTextNode(std::to_string(blockSize));        
    blockSizeElement->appendChild((Poco::XML::Node *)blockSizeText);

    std::ostringstream ostr;
    Poco::XML::DOMWriter writer;
    writer.writeNode(ostr, doc);    
    doc->release();
    //cout << ostr.str() << endl;

    /* Redirect request to NDP port on datanode */    
    uri = Poco::URI(nameNodeResp.get("Location"));
    uri.setPort(std::stoi(dikeConfig["dike.dfs.ndp.http-port"]));

    queryParameters = uri.getQueryParameters();
    /* Remove offset from parameters */
    QueryParameters::iterator it = queryParameters.begin();
    while (it != queryParameters.end()) {
        if((*it).first == "offset") {
            queryParameters.erase(it);
            break;
        }
        ++it;
    }

    uri.setQueryParameters(queryParameters);
    uri.addQueryParameter("offset", std::to_string(offset));

    HTTPRequest dataNodeReq;
    dataNodeReq.setHost(uri.getHost(), uri.getPort());
    dataNodeReq.setMethod("GET");
    dataNodeReq.setURI(uri.getPath() + "?" + uri.getRawQuery());
    dataNodeReq.set("ReadParam", ostr.str());
    HTTPClientSession dataNodeSession(uri.getHost(), uri.getPort());    
    dataNodeSession.sendRequest(dataNodeReq);
    HTTPResponse dataNodeResp;
    std::istream& fromHDFS = dataNodeSession.receiveResponse(dataNodeResp);

    DikeByteBuffer dbb = DikeByteBuffer(); 
    dbb.copyStreamToAWS(fromHDFS, toClient);
    dbb.SendEnd(toClient);
    toClient.flush();

    if(verbose) {
        cout << DikeUtil().Yellow() << DikeUtil().Now() << " Done " << DikeUtil().Reset();
        cout << DikeUtil().Red() << "Total bytes " << dbb.m_TotalBytes << " " << DikeUtil().Reset() << endl;       
    }
}
