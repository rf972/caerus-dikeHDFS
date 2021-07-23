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

public class DikeClient
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

        if(fname.endsWith(".csv")) {
            perfTest(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
        } else {            
            //parquetTest(dikehdfsPath, fname, conf, false/*pushdown*/, false/*partitionned*/);
            //parquetTest(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
            
            binaryColumnTest(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
        }
        //perfTest(dikehdfsPath, fname, conf, true /*pushdown*/, true/*partitionned*/);
        
        //perfTest(dikehdfsPath, fname, conf, false/*pushdown*/, false/*partitionned*/);        
        //Validate(dikehdfsPath, fname, conf);
    }

    public static String getReadParam(String name,
                                      long blockSize) throws XMLStreamException
    {
        if(name.endsWith(".csv")) {
            return getCsvReadParam(name, blockSize);
        } else if(name.endsWith(".parquet")) {
            return getParquetReadParam(name, blockSize);
        }
        return null;
    }
 

    public static String getCsvReadParam(String name,
                                      long blockSize) throws XMLStreamException 
    {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("dikeSQL");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("Query");
        //xmlw.writeCData("SELECT * FROM S3Object");
        // xmlw.writeCData("SELECT s._1, s._2, _16 FROM S3Object s");
        String query = "SELECT s._1, s._2, _16 FROM S3Object";
        String dikeQuery = System.getenv("DIKE_QUERY");
        if(dikeQuery != null){
            query = dikeQuery;
        }
        xmlw.writeCData(query);
        xmlw.writeEndElement(); // Query

        xmlw.writeStartElement("BlockSize");
        xmlw.writeCharacters(String.valueOf(blockSize));
        xmlw.writeEndElement(); // BlockSize

        xmlw.writeStartElement("HeaderInfo");
        xmlw.writeCharacters("IGNORE"); // USE , IGNORE or NONE
        xmlw.writeEndElement(); // HeaderInfo

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();

        return strw.toString();
    }

    public static String getParquetReadParam(String name,
                                      long blockSize) throws XMLStreamException 
    {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");
        
        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("dikeSQL.parquet");
        xmlw.writeEndElement(); // Name
        
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("Query");
        //xmlw.writeCData("SELECT * FROM S3Object");
        //xmlw.writeCData("SELECT l_orderkey, l_linenumber, l_quantity, l_shipdate, l_comment  FROM S3Object");
        //xmlw.writeCData("SELECT SUM(CAST(l_extendedprice as NUMERIC) * CAST(l_discount as NUMERIC)) FROM S3Object s WHERE l_shipdate IS NOT NULL AND l_shipdate >= '1994-01-01' AND l_shipdate < '1996-02-01' AND CAST(l_quantity as NUMERIC) < 50.0");

        String query = "SELECT l_extendedprice, l_discount, l_shipdate, l_quantity  FROM S3Object s WHERE l_shipdate IS NOT NULL AND l_shipdate >= '1994-01-01' AND l_shipdate < '1996-02-01' AND CAST(l_quantity as NUMERIC) < 50.0";
        String dikeQuery = System.getenv("DIKE_QUERY");
        if(dikeQuery != null){
            query = dikeQuery;
        }

        xmlw.writeCData(query);

        xmlw.writeEndElement(); // Query

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

    public static void perfTest(Path fsPath, String fname, Configuration conf)
    {
        perfTest(fsPath, fname, conf, false, false);
    }

    public static void perfTest(Path fsPath, String fname, Configuration conf, Boolean pushdown, Boolean partitionned)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;        
        NdpHdfsFileSystem dikeFS = null;
        ByteBuffer bb = ByteBuffer.allocate(1024);
        long totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;
        Map<String,Statistics> stats;
        int traceRecordCount = 10;

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
            
            FSDataInputStream dataInputStream = null;

            if (partitionned) { 
                BlockLocation[] locs = fs.getFileBlockLocations(fileToRead, 0, Long.MAX_VALUE);                            
                for (int i  = 0; i < locs.length; i++) {
                    System.out.format("%d off=%d size=%d\n", i, locs[i].getOffset(), locs[i].getLength());

                    readParam = getReadParam(fname, locs[i].getLength());

                    if(fs.getScheme() == "ndphdfs"){
                        dikeFS = (NdpHdfsFileSystem)fs;
                        dataInputStream = dikeFS.open(fileToRead, 128 << 10, readParam);                    
                    }

                    dataInputStream.seek(locs[i].getOffset());
                    BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream,StandardCharsets.UTF_8), 128 << 10);
                    String record = br.readLine();
                    int counter = 0;
                    while (record != null && record.length() > 0 ) {
                        if(counter < 10) {
                            System.out.println(record);
                        }
/*                        
                        String[] values = record.split(",");
                        if(values.length < 15){
                            System.out.println(counter);
                            System.out.println(record);
                            return;
                        }
*/                    
                        totalDataSize += record.length() + 1; // +1 to count end of line
                        totalRecords += 1;
                        counter += 1;

                        record = br.readLine(); // Should be last !!!
                    }
                    br.close();                    
                }
            } else { // regular read
                if(pushdown){
                    dikeFS = (NdpHdfsFileSystem)fs;
                    readParam = getReadParam(fname, 0 /* ignore stream size */);                                        
                    dataInputStream = dikeFS.open(fileToRead, 128 << 10, readParam);                    
                } else {
                    dataInputStream = fs.open(fileToRead);
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream,StandardCharsets.UTF_8));
                String record;
                record = br.readLine();
                while (record != null){
                    if(totalRecords < traceRecordCount) {
                        System.out.println(record);
                    }

                    if (record.length() == 0){
                        System.out.println("Recieved zero length record");
                        break;
                    }
                    totalDataSize += record.length() + 1; // +1 to count end of line
                    totalRecords += 1;

                    record = br.readLine(); // Should be last !!!
                }
                br.close();            
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

    public static void Validate(Path fsPath, String fname, Configuration conf)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs1 = null;
        FileSystem fs2 = null;
        NdpHdfsFileSystem dikeFS1 = null;
        NdpHdfsFileSystem dikeFS2 = null;
        //ByteBuffer bb = ByteBuffer.allocate(1024);
        long totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;

        long start_time;

        try {
            fs1 = FileSystem.get(fsPath.toUri(), conf);
            fs2 = FileSystem.get(fsPath.toUri(), conf);
            dikeFS1 = (NdpHdfsFileSystem)fs1;
            dikeFS2 = (NdpHdfsFileSystem)fs2;
            System.out.println("\nConnected to -- " + fsPath.toString());
            start_time = System.currentTimeMillis();                        
            
            FSDataInputStream d1 = null;
            FSDataInputStream d2 = null;

            BlockLocation[] locs = fs1.getFileBlockLocations(fileToRead, 0, Long.MAX_VALUE);            
            readParam = getReadParam(fname, 0);
            d2 = dikeFS2.open(fileToRead, 128 << 10, readParam);
            BufferedReader br2 = new BufferedReader(new InputStreamReader(d2,StandardCharsets.UTF_8), 128 << 10);

            for (int i  = 0; i < locs.length; i++) {
                System.out.format("%d off=%d size=%d\n", i, locs[i].getOffset(), locs[i].getLength());

                readParam = getReadParam(fname, locs[i].getLength());                                    
                d1 = dikeFS1.open(fileToRead, 128 << 10, readParam);

                d1.seek(locs[i].getOffset());
                BufferedReader br1 = new BufferedReader(new InputStreamReader(d1,StandardCharsets.UTF_8), 128 << 10);
                String record1 = br1.readLine();
                String record2 = br2.readLine();
                int counter = 0;
                while (record1 != null && record1.length() > 0 ) {
                    totalDataSize += record1.length() + 1; // +1 to count end of line
                    totalRecords += 1;
                    counter += 1;
  
                    if(!record1.equals(record2)) {
                        System.out.format("P %d: %s\n",totalRecords, record1);
                        System.out.format("O %d: %s\n",totalRecords, record2);
                        //System.out.format("At line %d totalRecords %d\n", counter, totalRecords);
                        br1.close(); 
                        br2.close();
                        return;
                    }  
                    
                    record1 = br1.readLine(); 
                    if(record1.length() > 0 && record2 != null){
                        record2 = br2.readLine();
                    }
                }                
                br1.close();
                d1.close();
            }
            br2.close();
            d2.close();
            fs1.close();
            fs2.close();
        } catch (Exception ex) {
            System.out.println("Exception !!! ");
            ex.printStackTrace();
            return;
        }
        
        long end_time = System.currentTimeMillis();

        Map<String,Statistics> stats = fs1.getStatistics();
        //System.out.println(fs.getScheme());
        System.out.format("BytesRead %d\n", stats.get(fs1.getScheme()).getBytesRead());
        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }


    public static void parquetTest(Path fsPath, String fname, Configuration conf, Boolean pushdown, Boolean partitionned)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;        
        NdpHdfsFileSystem dikeFS = null;
        ByteBuffer bb = ByteBuffer.allocate(1024);
        long totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;
        Map<String,Statistics> stats;
        int traceRecordCount = 10;

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
            
            FSDataInputStream dataInputStream = null;

            // regular read
            if(pushdown){
                dikeFS = (NdpHdfsFileSystem)fs;
                readParam = getReadParam(fname, 0 /* ignore stream size */);                                        
                dataInputStream = dikeFS.open(fileToRead, 128 << 10, readParam);                    
            } else {
                dataInputStream = fs.open(fileToRead);
            }

            //BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream,StandardCharsets.UTF_8));
  
            DataInputStream dis = new DataInputStream(new BufferedInputStream(dataInputStream, 64*1024 ));

            int dataTypes[];
            long nCols = dis.readLong();
            System.out.println("nCols : " + String.valueOf(nCols));
            dataTypes = new int [(int)nCols];
            for( int i = 0 ; i < nCols; i++){
                dataTypes[i] = (int)dis.readLong();
                System.out.println(String.valueOf(i) + " : " + String.valueOf(dataTypes[i]));
            }

            final int TYPE_INT64 = 1;
            final int TYPE_DOUBLE = 2;
            final int TYPE_BYTE_ARRAY = 3;
            byte buffer[] = new byte[256];
            String record = "";

            while(true) {
                try {
                    record = "";
                    for( int i = 0 ; i < nCols; i++) {
                        switch(dataTypes[i]) {
                            case TYPE_INT64:
                            long int64_data = dis.readLong();
                            if(totalRecords < traceRecordCount) {
                                record += String.valueOf(int64_data) + ",";
                            }
                            break;
                            case TYPE_DOUBLE:
                            double double_data = dis.readDouble();
                            if(totalRecords < traceRecordCount) {
                                record += String.valueOf(double_data) + ",";
                            }
                            break;
                            case TYPE_BYTE_ARRAY:
                            /*
                            for(int j = 0 ; j < 256; j++) {
                                buffer[j] = (byte)dis.readUnsignedByte();
                                if(buffer[j] == 0) {
                                    break;
                                }
                            }
                            */
                            String s = dis.readUTF();
                            if(totalRecords < traceRecordCount) {
                                //String s = new String(buffer, StandardCharsets.UTF_8);
                                record += s + ",";
                            }
                            break;
                        }
                    } 
                    if(totalRecords < traceRecordCount) {
                        System.out.println(record);
                    }
                    totalRecords++;
                } catch (Exception ex) {
                    System.out.println(ex);
                    break;
                }
            }
/*            
            int rc;
            do {                
                rc = dataInputStream.skipBytes(8192);
            } while(rc > 0);
*/
            
/*            
            byte buffer[] = new byte[8192];
            ByteArrayOutputStream bos  = new ByteArrayOutputStream(1 << 20);
            int rc;
            do {                
                rc = dataInputStream.read(buffer, 0, 8192);
                if (rc > 0) {
                    bos.write(buffer, 0, rc);
                }
            } while(rc >= 0);
            
            byte [] fileData = bos.toByteArray();
            totalDataSize = fileData.length;            
            //String hex = javax.xml.bind.DatatypeConverter.printHexBinary(fileData);
            //System.out.println(hex);
*/            

            class MySeekableInputStream extends SeekableInputStream {
                public byte[] buffer;
                public long pos = 0;

                public MySeekableInputStream(byte[] buf) {
                    buffer = buf;
                }

                @Override
                public long getPos() throws IOException {
                    System.out.println("MySeekableInputStream::getPos ");
                    return pos;
                }

                @Override
                public void seek(long newPos) throws IOException {
                    System.out.println("MySeekableInputStream::seek " + newPos);
                    pos = newPos;
                }

                @Override
                public int read() throws IOException {
                    System.out.println("MySeekableInputStream::read 485");
                    int b =  buffer[(int)pos];
                    pos++;
                    return (b & 0xff);
                }

                @Override
                public  void readFully(byte[] bytes) throws IOException {
                    System.out.println("MySeekableInputStream::readFully 490");
                    System.arraycopy(buffer, 0, bytes, 0, bytes.length);
                }

                @Override
                public void readFully(byte[] bytes, int start, int len) throws IOException{
                    throw new IOException();
                }

                @Override
                public void readFully(ByteBuffer buf) throws IOException {
                    //readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
                    System.out.println("MySeekableInputStream::readFully 507");
                    System.arraycopy(buffer, (int)pos, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
                    buf.position(buf.limit());                    
                }

                @Override
                public int read(ByteBuffer buf) throws IOException {
                    //throw new IOException();
                    System.out.println("MySeekableInputStream::read 506");
                    return 0;
                }
            }

            class MyInputFile implements InputFile {
                public byte [] fileData;
                public MyInputFile (byte [] fileData) {
                   this.fileData = fileData;
                }
                public long getLength() throws IOException {
                    System.out.println("MyInputFile::getLength " + fileData.length);
                    return fileData.length;                    
                }
                public SeekableInputStream newStream() throws IOException {
                    return new MySeekableInputStream(fileData);
                }
            }
            /*
            MyInputFile myInputFile = new MyInputFile(fileData);
            ParquetFileReader reader = ParquetFileReader.open(myInputFile);
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType	schema = fileMetaData.getSchema();
            
            ParquetMetadata parquetMetadata = reader.getFooter();
            //System.out.println("parquetMetadata : " + ParquetMetadata.toPrettyJSON(parquetMetadata));
            System.out.println("RecordCount : " +  reader.getRecordCount());
            List<BlockMetaData>	blocks = parquetMetadata.getBlocks();
            for (BlockMetaData block : blocks) {
                List<ColumnChunkMetaData> columns = block.getColumns() ;
                for (ColumnChunkMetaData colum : columns) {
                    System.out.println(colum.toString());
                }                
            }
            */

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

    public static void binaryColumnTest(Path fsPath, String fname, Configuration conf, Boolean pushdown, Boolean partitionned)
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
            readParam = getReadParam(fname, 0 /* ignore stream size */);                                        
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
            final int TYPE_INT64 = 1;
            final int TYPE_DOUBLE = 2;
            final int TYPE_BYTE_ARRAY = 3;
  
            class ColumVector {                
                ByteBuffer   byteBuffer = null;
                LongBuffer   longBuffer = null;
                DoubleBuffer doubleBuffer = null;
                byte text_buffer[] = null;
                int text_size;
                int index_buffer [] = null;
                int data_type;
                int record_count;
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
                            text_buffer = new byte[BATCH_SIZE * 64];
                            index_buffer = new int[BATCH_SIZE];
                        break;
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

            while(true) {
                try {
                    for( int i = 0 ; i < nCols; i++) {
                        columVector[i].readColumn(dis);
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
// export DIKE_QUERY="SELECT l_extendedprice, l_discount, l_shipdate, l_quantity  FROM S3Object"
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.parquet

// Q1 test
// export DIKE_TRACE_RECORD_COUNT=7000000
// export DIKE_QUERY="SELECT CAST(l_quantity as NUMERIC),CAST(l_extendedprice as NUMERIC),CAST(l_discount as NUMERIC),CAST(l_tax as NUMERIC),l_returnflag,l_linestatus FROM S3Object s WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"
// export DIKE_QUERY="SELECT l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus, l_shipdate FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"
// export DIKE_QUERY="SELECT _5, _6, _7, _8, _9, _10, _11 FROM S3Object WHERE _11 IS NOT NULL AND _11 <= '1998-09-02' LIMIT 1000"
// export DIKE_QUERY="SELECT count(*) FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"
// export DIKE_QUERY="SELECT MIN(l_shipdate) FROM S3Object"

// 1.2 sec
// export DIKE_QUERY="SELECT count(*) FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"

// 1.5 sec
// export DIKE_QUERY="SELECT min(l_quantity) FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"

// 3.796 sec
// export DIKE_QUERY="SELECT min(l_quantity), min(l_extendedprice), min(l_discount), min(l_tax),min(l_returnflag), min(l_linestatus), l_shipdate FROM S3Object WHERE l_shipdate IS NOT NULL AND l_shipdate <= '1998-09-02'"
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem_srg.parquet

// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.csv
// 
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl
// for i in $(seq 1 10); do echo $i && java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl; done



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