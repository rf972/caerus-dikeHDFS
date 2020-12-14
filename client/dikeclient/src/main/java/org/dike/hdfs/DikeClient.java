package org.dike.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

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

public class DikeClient
{
    public static void main( String[] args )
    {
        String fname = args[0];
        // Suppress log4j warnings
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        /*
        Logger.getRootLogger().setLevel(Level.DEBUG);
        Logger logger = Logger.getLogger("org.apache.hadoop.fs.FileSystem");

        //The log4j.properties file is placed under the resources folder of the project
        //log4j.rootLogger=DEBUG,console,file
        //log4j.appender.console=org.apache.log4j.ConsoleAppender
        // PropertiesConfigurator is used to configure logger from properties file
        //PropertyConfigurator.configure("log4j.properties");

        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        //add appender to any Logger (here is root)
        //Logger.getRootLogger().addAppender(console);
        logger.addAppender(console);
         */

        Configuration conf = new Configuration();
        Path hdfsCoreSitePath = new Path("/home/peter/config/core-site.xml");
        Path hdfsHDFSSitePath = new Path("/home/peter/config/hdfs-site.xml");
        conf.addResource(hdfsCoreSitePath);
        conf.addResource(hdfsHDFSSitePath);
        InputStream input = null;
        Path fileToRead = new Path(fname);
        try {
            System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));
            FileSystem fs = FileSystem.get(conf);
            input = fs.open(fileToRead);
            IOUtils.copyBytes(input, System.out, 4096);
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            return;
        } finally {
            IOUtils.closeStream(input);
        }

        try {
            //FileSystem fs = FileSystem.get(conf);
            DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
            DFSClient dfsClient = fs.getClient();
            DFSInputStream dfsInputStream = dfsClient.open(fileToRead.toString(), 4096, true);
            LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(fileToRead.toString(), 0);
            System.out.println(locatedBlocks.toString());
            for (int i = 0; i < locatedBlocks.locatedBlockCount(); i++) {
                LocatedBlock locatedBlock = locatedBlocks.get(i);
                System.out.println("Block [" + i + "]\n" + locatedBlock.toString());
            }

            IOUtils.copyBytes(dfsInputStream, System.out, 4096);
            IOUtils.closeStream(dfsInputStream);
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            return;
        }
    }
}

// java -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt
//
// java -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000 -Xmx1g -classpath target/dikeclient-1.0-jar-with-dependencies.jar org.dike.hdfs.DikeClient /test.txt

/*
doIO:143, SocketIOWithTimeout (org.apache.hadoop.net)
read:161, SocketInputStream (org.apache.hadoop.net)
readChannelFully:266, PacketReceiver (org.apache.hadoop.hdfs.protocol.datatransfer)
doReadFully:217, PacketReceiver (org.apache.hadoop.hdfs.protocol.datatransfer)
doRead:144, PacketReceiver (org.apache.hadoop.hdfs.protocol.datatransfer)
receiveNextPacket:112, PacketReceiver (org.apache.hadoop.hdfs.protocol.datatransfer)
readNextPacket:187, BlockReaderRemote (org.apache.hadoop.hdfs.client.impl)
read:146, BlockReaderRemote (org.apache.hadoop.hdfs.client.impl)
readFromBlock:118, ByteArrayStrategy (org.apache.hadoop.hdfs)
readBuffer:828, DFSInputStream (org.apache.hadoop.hdfs)
readWithStrategy:893, DFSInputStream (org.apache.hadoop.hdfs)
read:957, DFSInputStream (org.apache.hadoop.hdfs)
read:100, DataInputStream (java.io)
copyBytes:94, IOUtils (org.apache.hadoop.io)
main:46, DikeClient (org.dike.hdfs)
 */

/*
write:158, SocketOutputStream (org.apache.hadoop.net)
write:116, SocketOutputStream (org.apache.hadoop.net)
flushBuffer:81, BufferedOutputStream (java.io)
flush:142, BufferedOutputStream (java.io)
flush:123, DataOutputStream (java.io)
send:83, Sender (org.apache.hadoop.hdfs.protocol.datatransfer)
readBlock:116, Sender (org.apache.hadoop.hdfs.protocol.datatransfer)
newBlockReader:405, BlockReaderRemote (org.apache.hadoop.hdfs.client.impl)
getRemoteBlockReader:858, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
getRemoteBlockReaderFromTcp:754, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
build:381, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
getBlockReader:755, DFSInputStream (org.apache.hadoop.hdfs)
blockSeekTo:685, DFSInputStream (org.apache.hadoop.hdfs)
readWithStrategy:884, DFSInputStream (org.apache.hadoop.hdfs)
read:957, DFSInputStream (org.apache.hadoop.hdfs)
read:100, DataInputStream (java.io)
copyBytes:94, IOUtils (org.apache.hadoop.io)
main:46, DikeClient (org.dike.hdfs)
 */

/*
writeDelimitedTo:88, AbstractMessageLite (org.apache.hadoop.thirdparty.protobuf)
send:82, Sender (org.apache.hadoop.hdfs.protocol.datatransfer)
readBlock:116, Sender (org.apache.hadoop.hdfs.protocol.datatransfer)
newBlockReader:405, BlockReaderRemote (org.apache.hadoop.hdfs.client.impl)
getRemoteBlockReader:858, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
getRemoteBlockReaderFromTcp:754, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
build:381, BlockReaderFactory (org.apache.hadoop.hdfs.client.impl)
getBlockReader:755, DFSInputStream (org.apache.hadoop.hdfs)
blockSeekTo:685, DFSInputStream (org.apache.hadoop.hdfs)
readWithStrategy:884, DFSInputStream (org.apache.hadoop.hdfs)
read:957, DFSInputStream (org.apache.hadoop.hdfs)
read:100, DataInputStream (java.io)
copyBytes:94, IOUtils (org.apache.hadoop.io)
main:46, DikeClient (org.dike.hdfs)
 */