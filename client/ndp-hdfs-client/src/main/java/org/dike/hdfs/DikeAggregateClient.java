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

public class DikeAggregateClient
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
                
        String dikePathEnv = System.getenv("DIKE_PATH");
        if(dikePathEnv != null){
            dikehdfsPath = new Path("ndphdfs://" + dikePathEnv + ":9860/");
        }

        switch(Integer.parseInt(testNumber)) {
           case 18:
                String fname = "/tpch-test-parquet/lineitem.parquet";                
                String dag = getQ18DAG(fname);

                if(args.length < 2) {
                    String readParam = ReadParam.GetReadParam(fname, "Lambda", 0, dag);
                    TpchTest(dikehdfsPath, fname, conf, readParam);
                    return;
                }

                String clearAllParam = ReadParam.GetReadParam(fname, "LambdaClearAll", 0, "");
                String readAheadParam = ReadParam.GetReadParam(fname, "LambdaReadAhead", 0, dag);
                String readTotalParam = ReadParam.GetReadParam(fname, "LambdaTotal", 0, dag);
                String getPartitionsParam = ReadParam.GetReadParam(fname, "LambdaInfo", 0, "");

                InitReadAheadProcessor(dikehdfsPath, fname, conf, clearAllParam);
                if(args[1].equals("ClearAll")){
                    return;
                }
                //InitReadAheadProcessor(dikehdfsPath, fname, conf, readAheadParam);
                InitReadAheadProcessor(dikehdfsPath, fname, conf, readTotalParam);
                InitReadAheadProcessor(dikehdfsPath, fname, conf, getPartitionsParam);
                try {                        
                    Thread.sleep(500);
                } catch (Exception e) {
                    System.out.println(e);
                }                
                for(int i = 0; i < Integer.parseInt(args[1]); i++) {
                    try {                        
                        Thread.sleep(50);
                    } catch (Exception e) {
                        System.out.println(e);
                    }   
                    System.out.println("===");
                    String readParam = ReadParam.GetReadParam(fname, "Lambda", i, dag);
                    TpchTest(dikehdfsPath, fname, conf, readParam);
                }
                
                break;
            default:
                System.out.format("Unsupported testNumber %d \n", Integer.parseInt(testNumber));
                return;            
        }        
    }        

    public static String getQ18DAG(String name)    
    {
        String out;
        out = "{\"Name\":\"DAG Projection\",\"NodeArray\":[";
        out += "{\"Name\":\"InputNode\",\"Type\":\"_INPUT\",\"File\":\"ndphdfs://dikehdfs/tpch-test-parquet//lineitem.parquet\"},";
        out += "{\"Type\":\"_FILTER\",\"FilterArray\":[{\"Expression\":\"IsNotNull\",\"Arg\":{\"ColumnReference\":\"l_orderkey\"}}],\"Name\":\"Filter Q18\"},";

        out += "{\"Name\":\"Aggregate Q18\", \"Type\":\"_AGGREGATE\",";
        out += "\"GroupingArray\":[{\"ColumnReference\":\"l_orderkey\"}],";
        out += "\"AggregateArray\":[{\"Aggregate\":\"sum\",\"Expression\":{\"ColumnReference\":\"l_quantity\"}}]},";

        out += "{\"Name\":\"Aggregate Q18 barrier\", \"Type\":\"_AGGREGATE\", \"Barrier\":\"1\",";
        out += "\"GroupingArray\":[{\"ColumnReference\":\"l_orderkey\"}],";
        out += "\"AggregateArray\":[{\"Aggregate\":\"sum\",\"Expression\":{\"ColumnReference\":\"sum(l_quantity)\"}}]},";
       
        out += "{\"Name\":\"OutputNode\",\"Type\":\"_OUTPUT\",\"CompressionType\":\"ZSTD\",\"CompressionLevel\":\"2\"}]}";        

        return out;        
    }

    public static void InitReadAheadProcessor(Path fsPath, String fname, Configuration conf, String readParam)
    {        
        Path fileToRead = new Path(fname);
        FileSystem fs = null;        
        NdpHdfsFileSystem dikeFS = null;
        final int BUFFER_SIZE = 128 * 1024;

        try {
            fs = FileSystem.get(fsPath.toUri(), conf);
            dikeFS = (NdpHdfsFileSystem)fs;            
            FSDataInputStream dataInputStream = dikeFS.open(fileToRead, BUFFER_SIZE, readParam);                                
            BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream,StandardCharsets.UTF_8), 128 << 10);
            String line = br.readLine();
            System.out.println(line);
            
        } catch (Exception ex) {
            System.out.println("Error occurred: ");
            ex.printStackTrace();
            return;
        }
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
            stats.get(fs.getScheme()).reset();
            
            start_time = System.currentTimeMillis();                                                
            dikeFS = (NdpHdfsFileSystem)fs;

            FSDataInputStream dataInputStream = dikeFS.open(fileToRead, BUFFER_SIZE, readParam);                    
            DataInputStream dis = new DataInputStream(new BufferedInputStream(dataInputStream, BUFFER_SIZE ));
            
            long nCols = dis.readLong();
            System.out.println("nCols : " + String.valueOf(nCols));
            if (nCols > 32) {
                return;
            }
            int dataTypes[] = new int [(int)nCols];
            for( int i = 0 ; i < nCols && i < 32; i++){
                dataTypes[i] = (int)dis.readLong();                
            }
            
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
        System.out.format("BytesRead %d\n", stats.get(fs.getScheme()).getBytesRead());
        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }    
}

// mvn package -o
// Q18
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeAggregateClient 18
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeAggregateClient 18 1

// export DIKE_TRACE_RECORD_MAX=36865
// export DIKE_COMPRESSION=ZSTD
// export DIKE_COMPRESSION_LEVEL=3
// export DIKE_PATH=DP3
