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

import javax.security.auth.login.LoginException;

// json related stuff
import java.io.StringWriter;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonWriter;

// Compression support
import java.io.UnsupportedEncodingException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

// LZ4 support
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHashFactory;

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

public class DikeLambdaClient
{
    public static void main( String[] args )
    {
        String fname = args[0];
        // Suppress log4j warnings
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Configuration conf = new Configuration();
        String userName = System.getProperty("user.name");
        Path hdfsCoreSitePath = new Path("/home/" + userName + "/config/core-client.xml");
        Path hdfsHDFSSitePath = new Path("/home/" + userName + "/config/hdfs-site.xml");
        conf.addResource(hdfsCoreSitePath);
        conf.addResource(hdfsHDFSSitePath);

        Path webhdfsPath = new Path("webhdfs://dikehdfs:9870/");
        Path dikehdfsPath = new Path("ndphdfs://dikehdfs:9860/");
        Path hdfsPath = new Path("hdfs://dikehdfs:9000/");
            
        //TpchQ1Test(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
        LambdaTest(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
    }    
    
    public static String getLambdaReadParam(String name) throws XMLStreamException
    {
        if(name.contains("lineitem")) {
            return getLambdaQ1ReadParam(name);
        }
        if(name.contains("nation")) {
            return getLambdaQ5ReadParam(name);
        }
        return null;
    }

    public static String getLambdaQ1ReadParam(String name) throws XMLStreamException 
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
        projectionNodeBuilder.add("Name", "TpchQ1");
        projectionNodeBuilder.add("Type", "_PROJECTION");
        JsonArrayBuilder projectionArrayBuilder = Json.createArrayBuilder();
        projectionArrayBuilder.add("l_quantity");
        projectionArrayBuilder.add("l_extendedprice");
        projectionArrayBuilder.add("l_discount");
        projectionArrayBuilder.add("l_tax");
        projectionArrayBuilder.add("l_returnflag");
        projectionArrayBuilder.add("l_linestatus");
        projectionArrayBuilder.add("l_shipdate");

        projectionNodeBuilder.add("ProjectionArray", projectionArrayBuilder);

        JsonObjectBuilder optputNodeBuilder = Json.createObjectBuilder();
        optputNodeBuilder.add("Name", "OutputNode");
        optputNodeBuilder.add("Type", "_OUTPUT");        

        String compressionType = "None";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

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
        projectionNodeBuilder.add("Name", "TpchQ1");
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

        String compressionType = "None";
        String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
        if(compressionTypeEnv != null){
            compressionType = compressionTypeEnv;
        }
        optputNodeBuilder.add("CompressionType", compressionType);

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

    public static void LambdaTest(Path fsPath, String fname, Configuration conf, Boolean pushdown, Boolean partitionned)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;        
        NdpHdfsFileSystem dikeFS = null;        
        long totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;
        Map<String,Statistics> stats;
        int traceRecordCount = 10;
        final int BUFFER_SIZE = 128 * 1024;

        String traceRecordCountEnv = System.getenv("DIKE_TRACE_RECORD_COUNT");
        if(traceRecordCountEnv != null){
            traceRecordCount = Integer.parseInt(traceRecordCountEnv);
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
            readParam = getLambdaReadParam(fname);                                        
            FSDataInputStream dataInputStream = dikeFS.open(fileToRead, BUFFER_SIZE, readParam);                    
  
            DataInputStream dis = new DataInputStream(new BufferedInputStream(dataInputStream, BUFFER_SIZE ));

            int dataTypes[];
            long nCols = dis.readLong();
            System.out.println("nCols : " + String.valueOf(nCols));
            dataTypes = new int [(int)nCols];
            for( int i = 0 ; i < nCols; i++){
                dataTypes[i] = (int)dis.readLong();
                System.out.println(String.valueOf(i) + " : " + String.valueOf(dataTypes[i]));
            }

            final int BATCH_SIZE = 4096;
            final int TYPE_INT64 = 2;
            final int TYPE_DOUBLE = 5;
            final int TYPE_BYTE_ARRAY = 6;
  
            class ColumVector {                
                ByteBuffer   byteBuffer = null;
                LongBuffer   longBuffer = null;
                DoubleBuffer doubleBuffer = null;
                byte text_buffer[] = null;
                int text_size;
                int index_buffer [] = null;
                int data_type;
                int record_count;
                Inflater inflater = new Inflater();
                byte [] compressedBuffer = new byte[256 * 1024];
                public ColumVector(int data_type){
                    this.data_type = data_type;
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
                            text_buffer = new byte[256 * 1024];
                            index_buffer = new int[BATCH_SIZE];
                        break;
                    }
                }

                public void readRawData(DataInputStream dis) throws IOException {
                    long nbytes = dis.readLong();
                    dis.readFully(compressedBuffer, 0, (int)nbytes);
                    if(data_type == TYPE_BYTE_ARRAY){
                        nbytes = dis.readLong();
                        dis.readFully(compressedBuffer, 0, (int)nbytes);
                    }
                }

                public void readColumn(DataInputStream dis, Inflater inflater) 
                    throws DataFormatException, UnsupportedEncodingException, IOException {
                    long nbytes = dis.readLong();
                    
                    dis.readFully(compressedBuffer, 0, (int)nbytes);                        
                    inflater.setInput(compressedBuffer);
                    int dataSize = inflater.inflate(byteBuffer.array());
                    inflater.reset();
                    record_count = (int) (dataSize / 8);

                    if(data_type == TYPE_BYTE_ARRAY){
                        record_count = (int) (nbytes);
                        int idx = 0;
                        for(int i = 0; i < record_count; i++){
                            index_buffer[i] = idx;
                            idx += byteBuffer.get(i) & 0xFF;
                        }
                        // Read actual text size                            
                        nbytes = dis.readLong();                    
                        dis.readFully(compressedBuffer, 0, (int)nbytes);                            
                        inflater.setInput(compressedBuffer);
                        dataSize = inflater.inflate(text_buffer);
                        inflater.reset();
                        text_size = dataSize;
                   }
                }

                public void readColumn(DataInputStream dis, LZ4SafeDecompressor decompressor) 
                    throws DataFormatException, UnsupportedEncodingException, IOException {
                    long nbytes = dis.readLong();
                    
                    dis.readFully(compressedBuffer, 0, (int)nbytes);
                    /* Note: if we know decompressed length
                    * We can call LZ4FastDecompressor :
                    * decompress(compressedBuffer, 0, byteBuffer.array(), 0, decompressedLength);
                    * for faster processing
                    */

                    // Compressed length is known (a little slower)
                    int dataSize = decompressor.decompress(compressedBuffer, 0, (int)nbytes, byteBuffer.array(), 0);

                    record_count = (int) (dataSize / 8);

                    if(data_type == TYPE_BYTE_ARRAY){
                        record_count = (int) (dataSize);
                        int idx = 0;
                        for(int i = 0; i < record_count; i++){
                            index_buffer[i] = idx;
                            idx += byteBuffer.get(i) & 0xFF;
                        }
                        // Read actual text size                            
                        nbytes = dis.readLong();                    
                        dis.readFully(compressedBuffer, 0, (int)nbytes);                            
                        //inflater.setInput(compressedBuffer);
                        //dataSize = inflater.inflate(text_buffer);
                        //inflater.reset();
                        dataSize = decompressor.decompress(compressedBuffer, 0, (int)nbytes, text_buffer, 0);
                        text_size = dataSize;
                   }
                }

                public void readColumn(DataInputStream dis) throws IOException {
                    long nbytes = dis.readLong();

                    record_count = (int) (nbytes / 8);
                    dis.readFully(byteBuffer.array(), 0, (int)nbytes);


                    if(data_type == TYPE_BYTE_ARRAY){
                        record_count = (int) (nbytes);
                        int idx = 0;
                        for(int i = 0; i < record_count; i++){
                            index_buffer[i] = idx;
                            idx += byteBuffer.get(i) & 0xFF;
                        }
                        // Read actual text size                            
                        nbytes = dis.readLong();                    
                            text_size = (int)nbytes;
                            dis.readFully(text_buffer, 0, (int)nbytes);
                    }
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
                            int len = byteBuffer.get(index) & 0xFF;
                            value = new String(text_buffer, index_buffer[index], len, StandardCharsets.UTF_8);
                        break;
                    }
                    return value;
                }
            };            
            
            ColumVector [] columVector = new ColumVector [(int)nCols];
            int record_count = 0;

            for( int i = 0 ; i < nCols; i++) {
                columVector[i] = new ColumVector(dataTypes[i]);
            }
            
            Boolean compressionEnabled = false;
            String compressionTypeEnv = System.getenv("DIKE_COMPRESSION");
            Inflater inflater = new Inflater();
            LZ4Factory factory = LZ4Factory.fastestInstance();
            //LZ4FastDecompressor decompressor = factory.fastDecompressor();
            LZ4SafeDecompressor decompressor = factory.safeDecompressor();

            if(compressionTypeEnv != null){
                if(compressionTypeEnv.equals("lz4")){
                    compressionEnabled = true;                    
                }
            }

            if(compressionEnabled){
                System.out.println("Compression ENABLED ");
            } else {
                System.out.println("Compression DISABLED ");
            }

            while(true) {
                try {
                    for( int i = 0 ; i < nCols; i++) {
                        if(compressionEnabled) {
                            //columVector[i].readColumn(dis, inflater);
                            //columVector[i].readRawData(dis);
                            columVector[i].readColumn(dis, decompressor);
                        } else {
                            columVector[i].readColumn(dis);
                        }
                    }
                    
                    if(totalRecords < traceRecordCount) {                        
                        for(int idx = 0; idx < traceRecordCount; idx++){
                            String record = "";
                            for( int i = 0 ; i < nCols; i++) {
                                record += columVector[i].getString(idx) + ",";
                            }
                            System.out.println(record);
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
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeLambdaClient /lineitem_srg.parquet
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeLambdaClient /nation.parquet

// for i in $(seq 1 500); do echo $i && java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeLambdaClient /lineitem_srg.parquet; done

/*
  required int64 field_id=1 l_orderkey;
  required int64 field_id=2 l_partkey;
  required int64 field_id=3 l_suppkey;
  required int64 field_id=4 l_linenumber;
  required double field_id=5 l_quantity;
  required double field_id=6 l_extendedprice;
  required double field_id=7 l_discount;
  required double field_id=8 l_tax;
  optional binary field_id=9 l_returnflag (String);
  optional binary field_id=10 l_linestatus (String);
  optional binary field_id=11 l_shipdate (String);
  optional binary field_id=12 l_commitdate (String);
  optional binary field_id=13 l_receiptdate (String);
  optional binary field_id=14 l_shipinstruct (String);
  optional binary field_id=15 l_shipmode (String);
  optional binary field_id=16 l_comment (String);
*/