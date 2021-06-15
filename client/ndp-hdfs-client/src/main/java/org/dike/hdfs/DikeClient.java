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
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.BlockLocation;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import org.apache.hadoop.io.IOUtils;

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

        perfTest(dikehdfsPath, fname, conf, true/*pushdown*/, false/*partitionned*/);
        //perfTest(dikehdfsPath, fname, conf, true /*pushdown*/, true/*partitionned*/);
        
        //perfTest(dikehdfsPath, fname, conf, false/*pushdown*/, false/*partitionned*/);        
        //Validate(dikehdfsPath, fname, conf);

        if(false){
            for(int i = 0; i < 10 ; i++){
                perfTest(dikehdfsPath, fname, conf, false/*pushdown*/, false/*partitionned*/); 
            }
        }
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
        xmlw.writeCData("SELECT s._1, s._2, _16 FROM S3Object s");
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
        xmlw.writeCData("SELECT l_orderkey, l_linenumber, l_quantity, l_shipdate, l_comment  FROM S3Object");
        xmlw.writeEndElement(); // Query

        xmlw.writeStartElement("RowGroupIndex");
        xmlw.writeCharacters("0");
        xmlw.writeEndElement(); // RowGroupIndex

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
                        if(counter < 5) {
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
                    if(totalRecords < 5) {
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
}

// mvn package -o
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.csv
// java -classpath target/ndp-hdfs-client-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.parquet
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl
// for i in $(seq 1 10); do echo $i && java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl; done
