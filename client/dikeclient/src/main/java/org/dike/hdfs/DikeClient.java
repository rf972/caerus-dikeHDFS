package org.dike.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.nio.ByteBuffer;

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


import org.apache.hadoop.hdfs.web.DikeHdfsFileSystem;

public class DikeClient
{
    public static void main( String[] args )
    {
        String fname = args[0];
        // Suppress log4j warnings
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Configuration conf = new Configuration();
        Path hdfsCoreSitePath = new Path("/home/peter/config/core-client.xml");
        Path hdfsHDFSSitePath = new Path("/home/peter/config/hdfs-site.xml");
        conf.addResource(hdfsCoreSitePath);
        conf.addResource(hdfsHDFSSitePath);

        Path webhdfsPath = new Path("webhdfs://dikehdfs:9870/");
        Path dikehdfsPath = new Path("dikehdfs://dikehdfs:9860/");
        Path hdfsPath = new Path("hdfs://dikehdfs:9000/");


        //perfTest(hdfsPath, fname, conf);
        //perfTest(hdfsPath, fname, conf);

        //perfTest(webhdfsPath, fname, conf);
        perfTest(dikehdfsPath, fname, conf);

    }

    public static void perfTest(Path fsPath, String fname, Configuration conf)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;
        DikeHdfsFileSystem dikeFS = null;
        ByteBuffer bb = ByteBuffer.allocate(1024);
        int totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;

        long start_time;

        try {
            fs = FileSystem.get(fsPath.toUri(), conf);
            System.out.println("\nConnected to -- " + fsPath.toString());
            start_time = System.currentTimeMillis();
            
            dikeFS = (DikeHdfsFileSystem)fs;

            XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
            StringWriter strw = new StringWriter();
            XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
            xmlw.writeStartDocument();
            xmlw.writeStartElement("Processor");
            //xmlw.writeAttribute("Name","dikeSQL");
            xmlw.writeStartElement("Name");
            xmlw.writeCharacters("dikeSQL");
            xmlw.writeEndElement(); // Name
            //xmlw.writeAttribute("Version","0.1");

            xmlw.writeStartElement("Configuration");
            xmlw.writeStartElement("Schema");
            xmlw.writeCharacters("l_orderkey INTEGER,l_partkey INTEGER,l_suppkey INTEGER,l_linenumber INTEGER,l_quantity NUMERIC,l_extendedprice NUMERIC,l_discount NUMERIC,l_tax NUMERIC,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment");
            xmlw.writeEndElement(); // Schema

            xmlw.writeStartElement("Query");
            xmlw.writeCData("SELECT * FROM S3Object");
            xmlw.writeEndElement(); // Query

            xmlw.writeEndElement(); // Configuration
            xmlw.writeEndElement(); // Processor
            xmlw.writeEndDocument();
            xmlw.close();

            readParam = strw.toString();
            //FSDataInputStream dataInputStream = dikeFS.open(fileToRead, 4096, readParam);
            FSDataInputStream dataInputStream = dikeFS.open(fileToRead, 4096);
            
            //FSDataInputStream dataInputStream = fs.open(fileToRead);

            BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream));
            String record;
            record = br.readLine();
            while (record != null){
                if(totalRecords < 5) {
                    System.out.println(record);
                }
                totalDataSize += record.length() + 1; // +1 to count end of line
                totalRecords += 1;

                record = br.readLine(); // Should be last !!!
            }
            br.close();
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            return;
        }

        long end_time = System.currentTimeMillis();

        Map<String,Statistics> stats = fs.getStatistics();
        System.out.println(fs.getScheme());
        //System.out.format("BytesRead %d\n", stats.get(fs.getScheme()).getBytesRead());

        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }
}

// mvn package -o
// java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt
