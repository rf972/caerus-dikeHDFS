package org.dike.hdfs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.ScanRange;

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.FileHeaderInfo;

import com.amazonaws.metrics.AwsSdkMetrics;

import java.io.File;
//import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.InetAddress;

import static com.amazonaws.util.IOUtils.copy;

public class NdpS3Client {

    /* This suppose to work for following path
      /tpch-test/lineitem.csv/part-00000-4b1396a9-a9f2-4951-822f-5e7df5ec5913-c000.csv
    */
    private static final String BUCKET_NAME = "tpch-test";    
    private static final String CSV_OBJECT_PREFIX = "lineitem.csv/";
    private static final String QUERY = "SELECT s._1, s._2, _16 FROM S3Object s";

    public static void main(String[] args) throws Exception {
        NdpS3Client client = new NdpS3Client();
        client.test(args);
    }
    
    public void test(String[] args) throws Exception {
        long totalDataSize = 0;
        int totalRecords = 0;

        String hostname = "dikehdfs";
        InetAddress inet = InetAddress.getByName(hostname);
        String IPAddress = inet.getHostAddress();   
        System.out.printf("IP address of host %s is %s %n", hostname, IPAddress);  
        
        if( AwsSdkMetrics.isMetricsEnabled()) {
            System.out.format("MetricsEnabled\n");
        } else {
            System.out.format("MetricsDisabled\n");
        }

        long start_time = System.currentTimeMillis();
        AmazonS3ClientBuilder.EndpointConfiguration endpointConfiguration = 
            new AmazonS3ClientBuilder.EndpointConfiguration("http://" + IPAddress + ":9858", "us-east-1");

        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(endpointConfiguration)
            .withClientConfiguration(new ClientConfiguration()
                .withRequestTimeout(24*3600*1000)
                .withSocketTimeout(24*3600*1000)
                //.withTcpKeepAlive(true)                
                .withClientExecutionTimeout(24*3600*1000))
            .build();

        long end_time = System.currentTimeMillis();
        System.out.format("It took %.3f sec to create s3Client\n", (end_time - start_time) / 1000.0);
        start_time = System.currentTimeMillis();

        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
            .withBucketName(BUCKET_NAME)
            .withPrefix(CSV_OBJECT_PREFIX)
            .withMaxKeys(1024);

        long fileSize = 0;
        long blockSize = 128 << 20;
        String objectKey = "";

        ListObjectsV2Result listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
        for (S3ObjectSummary objectSummary : listObjectsV2Result.getObjectSummaries()) {
            System.out.printf(" %s - %s (size: %d)\n", objectSummary.getBucketName(), objectSummary.getKey(), objectSummary.getSize());
            objectKey = objectSummary.getKey();
            fileSize = objectSummary.getSize();            
        }

        end_time = System.currentTimeMillis();
        System.out.format("It took %.3f sec to get Object List\n", (end_time - start_time) / 1000.0);
        start_time = System.currentTimeMillis();
        
        long readSize = 0;
        while (readSize < fileSize) {            
            long readEnd = Math.min(readSize + blockSize, fileSize);

            ScanRange scanRange = new ScanRange().withStart(readSize).withEnd(readEnd);                
            SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, objectKey, QUERY, scanRange);

            System.out.format("Reading from %d to  %d \n", readSize, readEnd);
            readSize = readEnd;

            SelectObjectContentResult result = s3Client.selectObjectContent(request);
            InputStream resultInputStream = result.getPayload().getRecordsInputStream();                
            
            BufferedReader br = new BufferedReader(new InputStreamReader(resultInputStream));
            String record = br.readLine();
            while (record != null) {
                totalDataSize += record.length() + 1; // +1 to count end of line
                totalRecords += 1;
                if(totalRecords < 5) {
                    System.out.println(record);
                }
                record = br.readLine();
            }
            br.close();
        } // Partition loop
        
        end_time = System.currentTimeMillis();        
        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }

    private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query, ScanRange scanRange) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.IGNORE));
        //inputSerialization.setCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.NONE));
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        request.setOutputSerialization(outputSerialization);

        request.setScanRange(scanRange);
        return request;
    }
}

// java -classpath target/ndp-s3-client-1.0-jar-with-dependencies.jar org.dike.hdfs.NdpS3Client