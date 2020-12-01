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
//import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;


public class DikeClient
{
    public static void main( String[] args )
    {
        //System.out.println( "Hello World!" );
        Configuration conf = new Configuration();
        Path hdfsCoreSitePath = new Path("/home/peter/config/core-site.xml");
        Path hdfsHDFSSitePath = new Path("/home/peter/config/hdfs-site.xml");
        conf.addResource(hdfsCoreSitePath);
        conf.addResource(hdfsHDFSSitePath);
        InputStream input = null;
        try {
            System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));
            FileSystem fs = FileSystem.get(conf);
            Path fileToRead = new Path("/test.txt");
            input = fs.open(fileToRead);
            IOUtils.copyBytes(input, System.out, 4096);
        } catch (Exception ex) {
            System.out.println("Error occurred while Configuring Filesystem ");
            ex.printStackTrace();
            return;
        } finally {
            IOUtils.closeStream(input);
        }
    }
}
