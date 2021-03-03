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
import java.lang.Runnable;


//import org.apache.hadoop.hdfs.web.NdpHdfsFileSystem;
import org.dike.hdfs.NdpHdfsFileSystem;

public class DikeClientTest implements Runnable 
{
    private String savedFname;
    public int exitCode = 0;
    public DikeClientTest(String fname) {
        savedFname = fname;
    }
    public void run() {
        try {
            //System.out.format("%s: start\n", Thread.currentThread().getName());
            test(savedFname);
        } catch (Exception e) {
            System.out.println("exception hit");
            exitCode = 1;
            throw e;
        }
    }
    public static void threadTest(int minThreads, int maxThreads, int iterations, String fname)
            throws java.lang.InterruptedException {
        
        /* Repeat the following test for i iterations
         * with t threads.
         */
        for (int repeat = 0; repeat < iterations; repeat ++) {
            System.out.format("%s: %s - start Iteration: %d\n",
                              Thread.currentThread().getName(),
                              fname, repeat);
            Thread testThreads[] = new Thread[maxThreads];
            DikeClientTest clientTest[] = new DikeClientTest[maxThreads];
            /* Loop from the most stressful to least stressful test. */
            for (int t = maxThreads; t >= minThreads; t--) {
                System.out.format("%s: %s - start threads: %d\n",
                                  Thread.currentThread().getName(), fname, t);
                for (int i = 0; i < t; i++) {
                    clientTest[i] = new DikeClientTest(fname);
                    testThreads[i] = new Thread(clientTest[i]);
                    testThreads[i].start();
                }
                for (int i = 0; i < t; i++) {
                    testThreads[i].join();
                    if (clientTest[i].exitCode != 0) {
                        System.exit(1);
                    }
                }
            }
        }
    }
    public void test(String fname) {
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
      
        //perfTest(webhdfsPath, fname, conf, false /*pushdown*/, false/*partitioned*/);
        //perfTest(dikehdfsPath, fname, conf, false /*pushdown*/, false/*partitioned*/);
        //perfTest(dikehdfsPath, fname, conf, true /*pushdown*/, false/*partitioned*/);
        Validate(dikehdfsPath, fname, conf);
    }

    public static String getReadParam(String name,
                                      long blockSize) throws XMLStreamException 
    {
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
        if (name.contains("customer")) {
            xmlw.writeCharacters("c_custkey LONG, c_name STRING, c_address STRING, c_nationkey LONG, c_phone STRING, c_acctbal NUMERIC, c_mktsegment STRING, c_comment STRING");
        } else if (name.contains("lineitem")) {
            xmlw.writeCharacters("l_orderkey INTEGER,l_partkey INTEGER,l_suppkey INTEGER,l_linenumber INTEGER,l_quantity NUMERIC,l_extendedprice NUMERIC,l_discount NUMERIC,l_tax NUMERIC,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment");
        } else if (name.contains("nation")) {
            xmlw.writeCharacters("n_nationkey LONG, n_name STRING, n_regionkey LONG, n_comment STRING");
        } else if (name.contains("order")) {
            xmlw.writeCharacters("o_orderkey LONG, o_custkey LONG, o_orderstatus STRING, o_totalprice NUMERIC, o_orderdate STRING, o_orderpriority STRING, o_clerk STRING, o_shippriority LONG, o_comment STRING");
        } else if (name.contains("partsupp")) {
            xmlw.writeCharacters("ps_partkey LONG, ps_suppkey LONG, ps_availqty LONG, ps_supplycost NUMERIC, ps_comment STRING");
        } else if (name.contains("part")) {
            xmlw.writeCharacters("p_partkey INTEGER,p_name, p_mfgr, p_brand, p_type, p_size INTEGER, p_container, p_retailprice NUMERIC, p_comment");
        } else if (name.contains("region")) {
            xmlw.writeCharacters("r_regionkey LONG, r_name STRING, r_comment STRING");
        } else if (name.contains("suplier")) {
            xmlw.writeCharacters("s_suppkey LONG, s_name STRING, s_address STRING, s_nationkey LONG, s_phone STRING, s_acctbal NUMERIC, s_comment STRING");
        }
        xmlw.writeEndElement(); // Schema

        xmlw.writeStartElement("Query");
        xmlw.writeCData("SELECT * FROM S3Object");
        //xmlw.writeCData("SELECT * FROM S3Object LIMIT 2 OFFSET 1");
        //xmlw.writeCData("SELECT COUNT(*) FROM S3Object");
        xmlw.writeEndElement(); // Query

        xmlw.writeStartElement("BlockSize");
        xmlw.writeCharacters(String.valueOf(blockSize));
        xmlw.writeEndElement(); // BlockSize

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();

        return strw.toString();
    }

    private void perfTest(Path fsPath, String fname, Configuration conf)
    {
        perfTest(fsPath, fname, conf, false, false);
    }

    private void perfTest(Path fsPath, String fname, Configuration conf, Boolean pushdown, Boolean partitioned)
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
        System.out.println(" starting pushdown:" + pushdown.toString() + " part: " + partitioned.toString());
        long start_time = System.currentTimeMillis();

        try {
            fs = FileSystem.get(fsPath.toUri(), conf);
            stats = fs.getStatistics();
            stats.get(fs.getScheme()).reset();

            System.out.println("\nConnected to -- " + fsPath.toString());
            start_time = System.currentTimeMillis();                        
            
            FSDataInputStream dataInputStream = null;

            if (partitioned) { 
                BlockLocation[] locs = fs.getFileBlockLocations(fileToRead, 0, Long.MAX_VALUE);                            
                for (int i  = 0; i < locs.length; i++) {
                    System.out.format("%d off=%d size=%d\n", i, locs[i].getOffset(), locs[i].getLength());

                    readParam = getReadParam(fname, locs[i].getLength());

                    if(fs.getScheme() == "ndphdfs"){
                        dikeFS = (NdpHdfsFileSystem)fs;
                        dataInputStream = dikeFS.open(fileToRead, 16 << 10, readParam);                    
                    }

                    dataInputStream.seek(locs[i].getOffset());
                    BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream), 16 << 10);
                    String record = br.readLine();
                    int counter = 0;
                    while (record != null && record.length() > 1 ) {
                        if(counter < 5) {
                            System.out.println(record);
                        }
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
                    dataInputStream = dikeFS.open(fileToRead, 16 << 10, readParam);                    
                } else {
                    dataInputStream = fs.open(fileToRead);
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream));
                String record;
                record = br.readLine();
                while (record != null && record.length() > 1){
                    if(totalRecords < 5) {
                        System.out.println(record);
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

    private void Validate(Path fsPath, String fname, Configuration conf)
    {
        InputStream input = null;
        Path fileToRead = new Path(fname);
        FileSystem fs = null;
        NdpHdfsFileSystem dikeFS = null;
        ByteBuffer bb = ByteBuffer.allocate(1024);
        long totalDataSize = 0;
        int totalRecords = 0;
        String readParam = null;

        long start_time;

        try {
            fs = FileSystem.get(fsPath.toUri(), conf);
            dikeFS = (NdpHdfsFileSystem)fs;
            System.out.println(Thread.currentThread().getName() +
                               " Connected to -- " + fsPath.toString());
            start_time = System.currentTimeMillis();                        
            
            FSDataInputStream d1 = null;
            FSDataInputStream d2 = null;

            BlockLocation[] locs = fs.getFileBlockLocations(fileToRead, 0, Long.MAX_VALUE);            
            readParam = getReadParam(fname, 0);
            d2 = dikeFS.open(fileToRead, 16 << 10, readParam);
            BufferedReader br2 = new BufferedReader(new InputStreamReader(d2), 16 << 10);

            for (int i  = 0; i < locs.length; i++) {
                System.out.format("%s: %d off=%d size=%d\n", 
                                  Thread.currentThread().getName(),
                                  i, locs[i].getOffset(), locs[i].getLength());

                readParam = getReadParam(fname, locs[i].getLength());                                    
                d1 = dikeFS.open(fileToRead, 16 << 10, readParam);

                d1.seek(locs[i].getOffset());
                BufferedReader br1 = new BufferedReader(new InputStreamReader(d1), 16 << 10);
                String record1 = br1.readLine();
                String record2 = br2.readLine();
                int counter = 0;
                while (record1 != null && record1.length() > 1 ) {
                    totalDataSize += record1.length() + 1; // +1 to count end of line
                    totalRecords += 1;
                    counter += 1;
                    if (Thread.currentThread().getName().contains("Thread-26") && (i > 5)) {
                        exitCode = 1;
                        return;
                    }
                    if(!record1.equals(record2)) {
                        System.out.format("%s: P %d: %s\n",
                                          Thread.currentThread().getName(), totalRecords, record1);
                        System.out.format("%s: O %d: %s\n",
                                          Thread.currentThread().getName(), totalRecords, record2);
                        //System.out.format("At line %d totalRecords %d\n", counter, totalRecords);
                        exitCode = 1;
                        return;
                    }  
                    
                    record1 = br1.readLine(); 
                    if(record1.length() > 1 && record2 != null){
                        record2 = br2.readLine();
                    }
                }
                br1.close();                    
            }
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            exitCode = 1;
            return;
        }

        long end_time = System.currentTimeMillis();

        Map<String,Statistics> stats = fs.getStatistics();
        //System.out.println(fs.getScheme());
        System.out.format("%s: BytesRead %d\n", Thread.currentThread().getName(), 
                          stats.get(fs.getScheme()).getBytesRead());
        System.out.format("%s: Received %d records (%d bytes) in %.3f sec\n",
                          Thread.currentThread().getName(), 
                          totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }    
}

// mvn package -o
// java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /lineitem.tbl
