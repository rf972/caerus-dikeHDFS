/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dike.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.EOFException;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.DoubleBuffer;
import java.nio.charset.StandardCharsets;

import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.Arrays;

import javax.security.auth.login.LoginException;

// json related stuff
import java.io.StringWriter;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonWriter;

// ZSTD support
import com.github.luben.zstd.Zstd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import org.apache.hadoop.io.IOUtils;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import org.apache.parquet.schema.MessageType;

//import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.ConsoleAppender;

// StaX XML imports
//import java.io.FileOutputStream;
import java.io.StringWriter;
import java.util.Iterator;
import javax.xml.stream.*;
import javax.xml.namespace.QName;

import org.dike.hdfs.NdpHdfsFileSystem;

public class DikeTpchClient
{
    public static void main( String[] args )
    {
        String testNumber = args[0];
        // Suppress log4j warnings
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Configuration conf = new Configuration();
        String userName = System.getProperty("user.name");
        Path hdfsCoreSitePath = new Path("/home/" + userName + "/config/core-client.xml");
        Path hdfsHDFSSitePath = new Path("/home/" + userName + "/config/hdfs-site.xml");
        conf.addResource(hdfsCoreSitePath);
        conf.addResource(hdfsHDFSSitePath);


        Path dikehdfsPath = new Path("ndphdfs://dikehdfs:9860/");
        //Path dikehdfsPath = new Path("ndphdfs://DP3:9860/");
        
        String dikePathEnv = System.getenv("DIKE_PATH");
        if(dikePathEnv != null){
            dikehdfsPath = new Path("ndphdfs://" + dikePathEnv + ":9860/");
        }

        String fname = "";
        String param = "";
        switch(Integer.parseInt(testNumber)) {
            case 1:
                //fname = "/lineitem_srg.parquet";
                fname = "/tpch-test-parquet/lineitem_1G.parquet";
                param = getQ1Param(fname);
            break;
            case 3:
                //fname = "/lineitem_srg.parquet";
                //fname = "/tpch-test-parquet/lineitem.parquet";
                //fname = "/tpch-test-parquet/lineitem_1G.parquet";
                fname = "/lineitem.parquet";
                param = getQ3Param(fname);
            break;
            case 6:
                fname = "/lineitem_srg.parquet";
                param = getQ6Param(fname);
            break;

            case 10:
                fname = "/lineitem_srg.parquet";
                param = getQ10_l_Param(fname);
            break;

            case 12:
                fname = "/lineitem_srg.parquet";
                param = getQ12Param(fname);
            break;

           case 14:
                fname = "/tpch-test-parquet/lineitem.parquet";
                param = getQ14Param(fname, Integer.parseInt(args[1]));
            break;

            case 21:
                fname = "/lineitem_srg.parquet";
                param = getQ21Param(fname);
            break;

            default:
                System.out.format("Unsupported testNumber %d \n", Integer.parseInt(testNumber));
                return;            
        }
        TpchTest(dikehdfsPath, fname, conf, param);
    }        

    public static String getQ1Param(String name)    
    {
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ1 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder();
        filterBuilder.add("Expression", "IsNotNull");
        JsonObjectBuilder argBuilder = Json.createObjectBuilder();
        argBuilder.add("ColumnReference", "l_shipdate");
        filterBuilder.add("Arg", argBuilder);        
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThanOrEqual");

        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_shipdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "1998-09-02");
        //argBuilder = Json.createObjectBuilder().add("Literal", "1898-09-02"); // Everything to filter out
        //argBuilder = Json.createObjectBuilder().add("Literal", "2998-09-02"); // Nothing to filter out
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 


        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ1 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_quantity");
        projectionArrayBuilder.add("l_extendedprice");
        projectionArrayBuilder.add("l_discount");
        projectionArrayBuilder.add("l_tax");
        projectionArrayBuilder.add("l_returnflag");
        projectionArrayBuilder.add("l_linestatus");
        //projectionArrayBuilder.add("l_shipdate");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }
    
    public static String getQ3Param(String name)    
    {
        /*
        Configuration.DAG = {
            "Name":"DAG Projection",
            "NodeArray":
            [
            {"Name":"InputNode","Type":"_INPUT","File":"/tpch-test-parquet/lineitem.parquet/part-00000-053e732d-0f83-4113-a6a0-96b25b6a6735-c000.snappy.parquet"},
            {"Type":"_FILTER",
            "FilterArray":
            [
                {"Expression":"IsNotNull","Arg":{"ColumnReference":"l_shipdate"}},
                {"Left":{"ColumnReference":"l_shipdate"},
                "Expression":"GreaterThan",
                "Right":{"Literal":"1995-03-15"}},
                {"Expression":"IsNotNull","Arg":{"ColumnReference":"l_orderkey"}}
            ],
            "Name":"TPC-H Test Q03"},
            {"Name":"TPC-H Test Q03","Type":"_PROJECTION", "ProjectionArray":["l_orderkey","l_extendedprice","l_discount"]},
            {"Name":"OutputNode","Type":"_OUTPUT","CompressionType":"ZSTD","CompressionLevel":"3"}
            ]}
        Configuration.RowGroupIndex = 1
        */
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ3 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();        

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThan");   
        filterBuilder.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_shipdate"));        
        filterBuilder.add("Right", Json.createObjectBuilder().add("Literal", "1995-03-15"));
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 

        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ3 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();

        projectionArrayBuilder.add("l_orderkey");
        projectionArrayBuilder.add("l_extendedprice");
        projectionArrayBuilder.add("l_discount");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("1");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }    

    public static String getQ6Param(String name)    
    { // SELECT  SUM( l_extendedprice) FROM S3Object WHERE l_shipdate IS NOT NULL AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24.0 AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' 
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ6 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder();
        filterBuilder.add("Expression", "IsNotNull");
        JsonObjectBuilder argBuilder = Json.createObjectBuilder();
        argBuilder.add("ColumnReference", "l_shipdate");
        filterBuilder.add("Arg", argBuilder);        
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThanOrEqual");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_discount");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "0.05");        
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);


        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThanOrEqual");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_discount");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "0.07");        
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_quantity");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "24.0");        
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThanOrEqual");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_shipdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "1994-01-01");        
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_shipdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "1995-01-01");        
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);



        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 


        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ6 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_extendedprice");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }

    public static String getQ10_l_Param(String name)    
    {
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "Tpch Q10 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder();
        filterBuilder.add("Expression", "IsNotNull");
        JsonObjectBuilder argBuilder = Json.createObjectBuilder();
        argBuilder.add("ColumnReference", "l_returnflag");
        filterBuilder.add("Arg", argBuilder);        
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "EqualTo");

        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_returnflag");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "R");
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 


        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "Tpch Q10 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_orderkey");
        projectionArrayBuilder.add("l_returnflag");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }



    public static String getLambdaQ10ReadParam(String name) throws XMLStreamException 
    {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        
        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ10");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("c_custkey");
        projectionArrayBuilder.add("c_name");
        projectionArrayBuilder.add("c_address");
        projectionArrayBuilder.add("c_nationkey");
        projectionArrayBuilder.add("c_phone");
        projectionArrayBuilder.add("c_acctbal");
        projectionArrayBuilder.add("c_comment");
        
        //"c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_comment"

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();
        nodeArrayBuilder.add(inputNodeBuilder.build());
        nodeArrayBuilder.add(projectionNodeBuilder.build());
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();

        return strw.toString();
    }

    public static String getLambdaQ5ReadParam(String name) throws XMLStreamException 
    {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        
        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ5");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        //["n_nationkey","n_name","n_regionkey"]
        projectionArrayBuilder.add("n_nationkey");
        projectionArrayBuilder.add("n_name");
        projectionArrayBuilder.add("n_regionkey");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();
        nodeArrayBuilder.add(inputNodeBuilder.build());
        nodeArrayBuilder.add(projectionNodeBuilder.build());
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();

        return strw.toString();
    }

public static String getQ12Param(String name)    
    {
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ12 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();        

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");   
        filterBuilder.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_commitdate"));        
        filterBuilder.add("Right", Json.createObjectBuilder().add("ColumnReference", "l_receiptdate"));
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");   
        filterBuilder.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_shipdate"));        
        filterBuilder.add("Right", Json.createObjectBuilder().add("ColumnReference", "l_commitdate"));
        filterArrayBuilder.add(filterBuilder);


        filterBuilder = Json.createObjectBuilder().add("Expression", "Or");
            JsonObjectBuilder l1 = Json.createObjectBuilder().add("Expression", "EqualTo");
            l1.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_shipmode"));
            l1.add("Right", Json.createObjectBuilder().add("Literal", "MAIL"));

            JsonObjectBuilder r1 = Json.createObjectBuilder().add("Expression", "EqualTo");
            r1.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_shipmode"));
            r1.add("Right", Json.createObjectBuilder().add("Literal", "SHIP"));
        filterBuilder.add("Left", l1);        
        filterBuilder.add("Right", r1);
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThanOrEqual");   
        filterBuilder.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_receiptdate"));        
        filterBuilder.add("Right", Json.createObjectBuilder().add("Literal", "1994-01-01"));
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");   
        filterBuilder.add("Left", Json.createObjectBuilder().add("ColumnReference", "l_receiptdate"));        
        filterBuilder.add("Right", Json.createObjectBuilder().add("Literal", "1995-01-01"));
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 

        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ12 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();        
        projectionArrayBuilder.add("l_orderkey");
        projectionArrayBuilder.add("l_shipdate");
        projectionArrayBuilder.add("l_commitdate");
        projectionArrayBuilder.add("l_receiptdate");
        projectionArrayBuilder.add("l_shipmode");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }    

    /* {"Name":"DAG Projection",
     "NodeArray":[
     {"Name":"InputNode","Type":"_INPUT","File":"/tpch-test-parquet/lineitem.parquet/part-00000-c498f3b7-c87f-4113-8e2f-0e5e0c99ccd5-c000.snappy.parquet"},
     {"Type":"_FILTER","FilterArray":[
         {"Expression":"IsNotNull","Arg":{"ColumnReference":"l_shipdate"}},
         {"Left":{"ColumnReference":"l_shipdate"},"Expression":"GreaterThanOrEqual","Right":{"Literal":"1995-09-01"}},
         {"Left":{"ColumnReference":"l_shipdate"},"Expression":"LessThan","Right":{"Literal":"1995-10-01"}},
         {"Expression":"IsNotNull","Arg":{"ColumnReference":"l_partkey"}}],"Name":"TPC-H Test Q14"},
     {"Name":"TPC-H Test Q14","Type":"_PROJECTION","ProjectionArray":["l_partkey","l_extendedprice","l_discount"]},
     {"Name":"OutputNode","Type":"_OUTPUT","CompressionType":"None","CompressionLevel":"-100"}]}
    */
    public static String getQ14Param(String name, int rgIndex)    
    {
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ14 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder();
        filterBuilder.add("Expression", "IsNotNull");
        JsonObjectBuilder argBuilder = Json.createObjectBuilder();
        argBuilder.add("ColumnReference", "l_shipdate");
        filterBuilder.add("Arg", argBuilder);        
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThanOrEqual");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_shipdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "1995-09-01");
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "LessThan");
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_shipdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("Literal", "1995-10-01");
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 

        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ14 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_partkey");
        projectionArrayBuilder.add("l_extendedprice");
        projectionArrayBuilder.add("l_discount");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        //xmlw.writeCharacters("100"); //rgIndex
        xmlw.writeCharacters(String.valueOf(rgIndex));
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }



    public static String getQ21Param(String name)    
    {
        try {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Lambda");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("DAG");
        JsonObjectBuilder dagBuilder = Json.createObjectBuilder();
        dagBuilder.add("Name", "DAG Projection");

        JsonArrayBuilder nodeArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder inputNodeBuilder = Json.createObjectBuilder();
        inputNodeBuilder.add("Name", "InputNode");
        inputNodeBuilder.add("Type", "_INPUT");
        inputNodeBuilder.add("File", name);
        nodeArrayBuilder.add(inputNodeBuilder.build());
        
        JsonObjectBuilder filterNodeBuilder = Json.createObjectBuilder();
        filterNodeBuilder.add("Name", "TpchQ21 Filter");
        filterNodeBuilder.add("Type", "_FILTER");
        JsonArrayBuilder filterArrayBuilder = Json.createArrayBuilder();

        JsonObjectBuilder filterBuilder = Json.createObjectBuilder();
        filterBuilder.add("Expression", "IsNotNull");
        JsonObjectBuilder argBuilder = Json.createObjectBuilder();
        argBuilder.add("ColumnReference", "l_commitdate");
        filterBuilder.add("Arg", argBuilder);        
        filterArrayBuilder.add(filterBuilder);

        filterBuilder = Json.createObjectBuilder().add("Expression", "GreaterThan");

        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_receiptdate");        
        filterBuilder.add("Left", argBuilder);
        argBuilder = Json.createObjectBuilder().add("ColumnReference", "l_commitdate");
        filterBuilder.add("Right", argBuilder);
        filterArrayBuilder.add(filterBuilder);

        filterNodeBuilder.add("FilterArray", filterArrayBuilder);
        
        nodeArrayBuilder.add(filterNodeBuilder.build()); 


        JsonObjectBuilder projectionNodeBuilder = Json.createObjectBuilder();
        projectionNodeBuilder.add("Name", "TpchQ21 Project");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_receiptdate");
        projectionArrayBuilder.add("l_commitdate");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        nodeArrayBuilder.add(projectionNodeBuilder.build());

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "ZSTD";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

        String compressionLevel = "2";
        String compressionLevelEnv = System.getenv("DIKE_COMPRESSION_LEVEL");
        if(compressionLevelEnv != null){
            compressionLevel = compressionLevelEnv;
        }
        optputNodeBuilder.add("CompressionLevel", compressionLevel);
        nodeArrayBuilder.add(optputNodeBuilder.build());        

        dagBuilder.add("NodeArray", nodeArrayBuilder);

        // For now we will assume simple pipe with ordered connections
        JsonObject dag = dagBuilder.build();

        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(dag);
        writer.close();

        xmlw.writeCharacters(stringWriter.getBuffer().toString());
        xmlw.writeEndElement(); // DAG

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

        xmlw.writeStartElement("LastAccessTime");
        xmlw.writeCharacters("1624464464409");
        xmlw.writeEndElement(); // LastAccessTime

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();
        return strw.toString();
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();            
        }
        return null;        
    }




    public static void TpchTest(Path fsPath, String fname, Configuration conf, String readParam)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;        
        NdpHdfsFileSystem dikeFS = null;        
        long totalDataSize = 0;
        int totalRecords = 0;        
        Map<String,Statistics> stats;
        int traceRecordMax = 10;
        int traceRecordCount = 0;
        final int BUFFER_SIZE = 128 * 1024;

        String traceRecordMaxEnv = System.getenv("DIKE_TRACE_RECORD_MAX");
        if(traceRecordMaxEnv != null){
            traceRecordMax = Integer.parseInt(traceRecordMaxEnv);
        }

        long start_time = System.currentTimeMillis();

        try {
            fs = FileSystem.get(fsPath.toUri(), conf);
            stats = fs.getStatistics();
            System.out.println("Scheme " + fs.getScheme());
            stats.get(fs.getScheme()).reset();

            System.out.println("\nConnected to -- " + fsPath.toString());
            start_time = System.currentTimeMillis();                                                

            dikeFS = (NdpHdfsFileSystem)fs;                                                              
            FSDataInputStream dataInputStream = dikeFS.open(fileToRead, BUFFER_SIZE, readParam);                    
            DataInputStream dis = new DataInputStream(new BufferedInputStream(dataInputStream, BUFFER_SIZE ));

            int dataTypes[];
            long nCols = dis.readLong();
            System.out.println("nCols : " + String.valueOf(nCols));
            if (nCols > 32) {
                return;
            }
            dataTypes = new int [(int)nCols];
            for( int i = 0 ; i < nCols && i < 32; i++){
                dataTypes[i] = (int)dis.readLong();
                System.out.println(String.valueOf(i) + " : " + String.valueOf(dataTypes[i]));
            }

            final int BATCH_SIZE = 256 << 10;
            final int TYPE_INT64 = 2;
            final int TYPE_DOUBLE = 5;
            final int TYPE_BYTE_ARRAY = 6;
            final int TYPE_FIXED_LEN_BYTE_ARRAY = 7;

            final int HEADER_DATA_TYPE = 0 * 4;
            final int HEADER_TYPE_SIZE = 1 * 4;
            final int HEADER_DATA_LEN = 2 * 4;
            final int HEADER_COMPRESSED_LEN = 3 * 4;

            class ColumVector {   
                int colId;             
                ByteBuffer   byteBuffer = null;
                LongBuffer   longBuffer = null;
                DoubleBuffer doubleBuffer = null;
                byte text_buffer[] = null;
                int text_size;
                int fixedTextLen = 0;
                int index_buffer [] = null;
                int data_type;
                int record_count;                
                byte [] compressedBuffer = new byte[BATCH_SIZE * 128];

                ByteBuffer header = null;

                public ColumVector(int colId, int data_type){
                    this.colId = colId;
                    this.data_type = data_type;
                    header = ByteBuffer.allocate(4 * 4); // Header size by int size
                    switch(data_type) {
                        case TYPE_INT64:
                            byteBuffer = ByteBuffer.allocate(BATCH_SIZE * 8);
                            longBuffer = byteBuffer.asLongBuffer();
                        break;
                        case TYPE_DOUBLE:
                            byteBuffer = ByteBuffer.allocate(BATCH_SIZE * 8);
                            doubleBuffer = byteBuffer.asDoubleBuffer();
                        break;
                        case TYPE_BYTE_ARRAY:
                            byteBuffer = ByteBuffer.allocate(BATCH_SIZE);
                            text_buffer = new byte[BATCH_SIZE * 128];
                            index_buffer = new int[BATCH_SIZE];
                        break;
                    }
                }

                public void readRawData(DataInputStream dis) throws IOException {
                    int nbytes;
                    dis.readFully(header.array(), 0, header.capacity());
                    nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
                    dis.readFully(compressedBuffer, 0, nbytes);
                    if(data_type == TYPE_BYTE_ARRAY){
                        dis.readFully(header.array(), 0, header.capacity());
                        nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
                        dis.readFully(compressedBuffer, 0, (int)nbytes);
                    }
                }

                public void readColumnZSTD(DataInputStream dis) throws  IOException {
                    int nbytes;
                    byte [] cb = null;
                    Boolean is_compressed;
                    
                    dis.readFully(header.array(), 0, header.capacity());
                    nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
                    //System.out.format("readColumn[%d] %d header size %d ", colId, nbytes, header.capacity());
                    //System.out.format("readColumn[%d] type %d ratio %f \n", colId, header.getInt(HEADER_DATA_TYPE), 1.0 * header.getInt(HEADER_DATA_LEN) /  header.getInt(HEADER_COMPRESSED_LEN));                    
                    if(nbytes > 0) {
                        is_compressed = true;
                    } else {
                        is_compressed = false;
                    }
                    if(is_compressed) {
                        cb = new byte [nbytes];
                        dis.readFully(cb, 0, nbytes);
                    } 

                    //System.out.format("readColumn[%d] %d decompressedSize %d \n", colId, nbytes, Zstd.decompressedSize(compressedBuffer));                    

                    int dataSize;
                    if(TYPE_FIXED_LEN_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
                        fixedTextLen = header.getInt(HEADER_TYPE_SIZE);                                                
                        dataSize = header.getInt(HEADER_DATA_LEN);
                        record_count = (int) (dataSize / fixedTextLen);
                        if(is_compressed){
                            Zstd.decompress(text_buffer, cb);
                        } else {                            
                            dis.readFully(text_buffer, 0, (int)dataSize);
                        }
                    } else {
                        fixedTextLen = 0;                        
                        dataSize = header.getInt(HEADER_DATA_LEN);
                        if(is_compressed){
                            Zstd.decompress(byteBuffer.array(), cb);
                        } else {
                            dis.readFully(byteBuffer.array(), 0, (int)dataSize);
                        }
                        //System.out.format("readColumn[%d] Zstd.decompress %d bytes \n", colId, nbytes);                        
                        record_count = (int) (dataSize / 8);
                    }

                    if(TYPE_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
                        record_count = (int) (dataSize);
                        int idx = 0;
                        for(int i = 0; i < record_count; i++){
                            index_buffer[i] = idx;
                            idx += byteBuffer.get(i) & 0xFF;
                        }
                        // Read actual text size                                                    
                        dis.readFully(header.array(), 0, header.capacity());
                        //System.out.format("readColumn[%d] type %d ratio %f \n", colId, header.getInt(HEADER_DATA_TYPE), 1.0 * header.getInt(HEADER_DATA_LEN) /  header.getInt(HEADER_COMPRESSED_LEN));

                        nbytes = header.getInt(HEADER_COMPRESSED_LEN);
                        dataSize = header.getInt(HEADER_DATA_LEN);

                        if(nbytes > 0){
                            cb = new byte [nbytes];
                            dis.readFully(cb, 0, nbytes);
                            Zstd.decompress(text_buffer, cb);
                        } else {
                            dis.readFully(text_buffer, 0, (int)dataSize);
                        }
                        text_size = dataSize;
                   }
                   //System.out.format("\n");
                }

                public void readColumn(DataInputStream dis) throws IOException {
                    long nbytes;
                            
                    dis.readFully(header.array(), 0, header.capacity());
                    nbytes = header.getInt(HEADER_DATA_LEN);
                    //System.out.format("readColumn[%d] %d header size %d ", colId, nbytes, header.capacity());

                    record_count = (int) (nbytes / 8);
                    
                    if(TYPE_FIXED_LEN_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
                        fixedTextLen = header.getInt(HEADER_TYPE_SIZE);
                        dis.readFully(text_buffer, 0, (int)nbytes);
                    } else {
                        fixedTextLen = 0;
                        dis.readFully(byteBuffer.array(), 0, (int)nbytes);
                    }
                    
                    if(TYPE_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)) {
                        record_count = (int) (nbytes);
                        int idx = 0;
                        for(int i = 0; i < record_count; i++){
                            index_buffer[i] = idx;
                            idx += byteBuffer.get(i) & 0xFF;
                        }
                        // Read actual text size                            
                        
                        dis.readFully(header.array(), 0, header.capacity());                                                
                        text_size =  header.getInt(HEADER_DATA_LEN);
                        //System.out.format("text_size %d ", text_size);
                        dis.readFully(text_buffer, 0, (int)text_size);
                    }
                    //System.out.format("\n");
                }

                public String getString(int index) {
                    String value = null;
                    switch(data_type) {
                        case TYPE_INT64:
                            value = String.valueOf(byteBuffer.getLong(index * 8));
                        break;
                        case TYPE_DOUBLE:
                            value = String.valueOf(byteBuffer.getDouble(index * 8));                            
                        break;
                        case TYPE_BYTE_ARRAY:
                            if(fixedTextLen > 0){
                                value = new String(text_buffer, fixedTextLen * index, fixedTextLen, StandardCharsets.UTF_8);
                            } else {
                                int len = byteBuffer.get(index) & 0xFF;
                                value = new String(text_buffer, index_buffer[index], len, StandardCharsets.UTF_8);
                            }
                        break;
                    }
                    return value;
                }
            };            
            
            ColumVector [] columVector = new ColumVector [(int)nCols];
            int record_count = 0;

            for( int i = 0 ; i < nCols; i++) {
                columVector[i] = new ColumVector(i, dataTypes[i]);
            }            

            while(true) {
                try {
                    for( int i = 0 ; i < nCols; i++) {
                        columVector[i].readColumnZSTD(dis);
                    }
                    
                    if(traceRecordCount < traceRecordMax) {                        
                        for(int idx = 0; idx < columVector[0].record_count && traceRecordCount < traceRecordMax; idx++){
                            String record = "";
                            for( int i = 0 ; i < nCols; i++) {
                                record += columVector[i].getString(idx) + ",";
                            }
                            System.out.println(record);
                            traceRecordCount++;
                        }                        
                    }
                    
                    totalRecords += columVector[0].record_count;
                    
                }catch (Exception ex) {
                    System.out.println(ex);
                    break;
                }
            }                          
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();
            long end_time = System.currentTimeMillis();            
            System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);             
            return;
        }

        long end_time = System.currentTimeMillis();
        
        //System.out.println(fs.getScheme());
        System.out.format("BytesRead %d\n", stats.get(fs.getScheme()).getBytesRead());
        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }    
}

// mvn package -o
// Q1
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 1
// Q3
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 3
// Q6
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 6
// Q5
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 5
// Q10
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 10
// Q12
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 12
// Q14
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 14
// Q21
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 21



// export DIKE_TRACE_RECORD_MAX=36865
// export DIKE_COMPRESSION=ZSTD
// export DIKE_COMPRESSION_LEVEL=3
// export DIKE_PATH=DP3

/*
for i in $(seq 0 2) ; do ( java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeTpchClient 14 $i & ); done

*/