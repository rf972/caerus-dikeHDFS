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

public class DikeClientTests
{
    private static String DefaultFiles[] = {
        "customer.tbl",
        "lineitem.tbl",
        "nation.tbl",
        "orders.tbl",
        "part.tbl",
        "partsupp.tbl",
        "region.tbl",
        "supplier.tbl"
    };
    public static void main( String[] args ) throws java.lang.InterruptedException {
        int minThreads = 1;
        int maxThreads = 8;
        int iterations = 1;

        // If no args are given, run test for each file.
        if (args.length == 0) {
            for (int i = 0; i < DefaultFiles.length; i++) {
                System.out.println(DefaultFiles[i]);
                DikeClientTest.threadTest(minThreads, maxThreads, iterations, 
                                          "/tpch-test/" + DefaultFiles[i]);
            }
        } else {
            // Otherwise run test for the input file name.
            String fname = args[0];
            DikeClientTest.threadTest(minThreads, maxThreads, iterations, fname);
        }
    } 
}

// mvn package -o
// To run all files:
// java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClientTests
// To run one file:
// java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClientTests /lineitem.tbl
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClientTests /lineitem.tbl
