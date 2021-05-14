package org.dike.hdfs;

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

    private static final String BUCKET_NAME = "tpch-test";
    private static final String CSV_OBJECT_KEY = "lineitem.csv";    
    private static final String QUERY = "SELECT s._1, s._2, _16 FROM S3Object s";

    public static void main(String[] args) throws Exception {
        long totalDataSize = 0;
        int totalRecords = 0;

        String hostname = "dikehdfs";
        InetAddress inet = InetAddress.getByName(hostname);
        String IPAddress = inet.getHostAddress();   
        System.out.printf("IP address of host %s is %s %n", hostname, IPAddress);  

        AmazonS3ClientBuilder.EndpointConfiguration endpointConfiguration = 
            new AmazonS3ClientBuilder.EndpointConfiguration("http://" + IPAddress + ":9858", "us-east-1");

        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(endpointConfiguration)
            .build();

        SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, CSV_OBJECT_KEY, QUERY);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);

        long start_time = System.currentTimeMillis();
        try (
            SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                    new SelectObjectContentEventVisitor() {
                       @Override
                        public void visit(SelectObjectContentEvent.EndEvent event)
                        {
                            isResultComplete.set(true);
                            System.out.println("Received End Event. Result is complete.");
                        }
                    }
                );

            //copy(resultInputStream, fileOutputStream);
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
        }

        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }
        long end_time = System.currentTimeMillis();        
        System.out.format("Received %d records (%d bytes) in %.3f sec\n", totalRecords, totalDataSize, (end_time - start_time) / 1000.0);
    }

    private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(new CSVInput());
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        request.setOutputSerialization(outputSerialization);

        ScanRange scanRange = new ScanRange().withStart(0).withEnd(42);
        request.setScanRange(scanRange);

        return request;
    }
}

// java -classpath target/ndp-s3-client-1.0-jar-with-dependencies.jar org.dike.hdfs.NdpS3Client