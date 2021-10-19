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

public class ColumVector {
    final int BATCH_SIZE = 256 << 10;
    final int TYPE_INT64 = 2;
    final int TYPE_DOUBLE = 5;
    final int TYPE_BYTE_ARRAY = 6;
    final int TYPE_FIXED_LEN_BYTE_ARRAY = 7;

    final int HEADER_DATA_TYPE = 0 * 4;
    final int HEADER_TYPE_SIZE = 1 * 4;
    final int HEADER_DATA_LEN = 2 * 4;
    final int HEADER_COMPRESSED_LEN = 3 * 4;
        
    int colId;             
    ByteBuffer   byteBuffer = null;
    LongBuffer   longBuffer = null;
    DoubleBuffer doubleBuffer = null;
    byte text_buffer[] = null;
    int text_size;
    int fixedTextLen = 0;
    int index_buffer [] = null;
    int data_type;
    int record_count;                
    byte [] compressedBuffer = new byte[BATCH_SIZE * 128];

    ByteBuffer header = null;

    public ColumVector(int colId, int data_type){
        this.colId = colId;
        this.data_type = data_type;
        header = ByteBuffer.allocate(4 * 4); // Header size by int size
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
                text_buffer = new byte[BATCH_SIZE * 128];
                index_buffer = new int[BATCH_SIZE];
            break;
        }
    }

    public void readRawData(DataInputStream dis) throws IOException {
        int nbytes;
        dis.readFully(header.array(), 0, header.capacity());
        nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
        dis.readFully(compressedBuffer, 0, nbytes);
        if(data_type == TYPE_BYTE_ARRAY){
            dis.readFully(header.array(), 0, header.capacity());
            nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
            dis.readFully(compressedBuffer, 0, (int)nbytes);
        }
    }

    public void readColumnZSTD(DataInputStream dis) throws  IOException {
        int nbytes;
        byte [] cb = null;
        Boolean is_compressed;
        
        dis.readFully(header.array(), 0, header.capacity());
        nbytes = header.getInt(HEADER_COMPRESSED_LEN);                    
        //System.out.format("readColumn[%d] %d header size %d ", colId, nbytes, header.capacity());
        //System.out.format("readColumn[%d] type %d ratio %f \n", colId, header.getInt(HEADER_DATA_TYPE), 1.0 * header.getInt(HEADER_DATA_LEN) /  header.getInt(HEADER_COMPRESSED_LEN));                    
        if(nbytes > 0) {
            is_compressed = true;
        } else {
            is_compressed = false;
        }
        if(is_compressed) {
            cb = new byte [nbytes];
            dis.readFully(cb, 0, nbytes);
        } 

        //System.out.format("readColumn[%d] %d decompressedSize %d \n", colId, nbytes, Zstd.decompressedSize(compressedBuffer));                    

        int dataSize;
        if(TYPE_FIXED_LEN_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
            fixedTextLen = header.getInt(HEADER_TYPE_SIZE);                                                
            dataSize = header.getInt(HEADER_DATA_LEN);
            record_count = (int) (dataSize / fixedTextLen);
            if(is_compressed){
                Zstd.decompress(text_buffer, cb);
            } else {                            
                dis.readFully(text_buffer, 0, (int)dataSize);
            }
        } else {
            fixedTextLen = 0;                        
            dataSize = header.getInt(HEADER_DATA_LEN);
            if(is_compressed){
                Zstd.decompress(byteBuffer.array(), cb);
            } else {
                dis.readFully(byteBuffer.array(), 0, (int)dataSize);
            }
            //System.out.format("readColumn[%d] Zstd.decompress %d bytes \n", colId, nbytes);                        
            record_count = (int) (dataSize / 8);
        }

        if(TYPE_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
            record_count = (int) (dataSize);
            int idx = 0;
            for(int i = 0; i < record_count; i++){
                index_buffer[i] = idx;
                idx += byteBuffer.get(i) & 0xFF;
            }
            // Read actual text size                                                    
            dis.readFully(header.array(), 0, header.capacity());
            //System.out.format("readColumn[%d] type %d ratio %f \n", colId, header.getInt(HEADER_DATA_TYPE), 1.0 * header.getInt(HEADER_DATA_LEN) /  header.getInt(HEADER_COMPRESSED_LEN));

            nbytes = header.getInt(HEADER_COMPRESSED_LEN);
            dataSize = header.getInt(HEADER_DATA_LEN);

            if(nbytes > 0){
                cb = new byte [nbytes];
                dis.readFully(cb, 0, nbytes);
                Zstd.decompress(text_buffer, cb);
            } else {
                dis.readFully(text_buffer, 0, (int)dataSize);
            }
            text_size = dataSize;
        }
        //System.out.format("\n");
    }

    public void readColumn(DataInputStream dis) throws IOException {
        long nbytes;
                
        dis.readFully(header.array(), 0, header.capacity());
        nbytes = header.getInt(HEADER_DATA_LEN);
        //System.out.format("readColumn[%d] %d header size %d ", colId, nbytes, header.capacity());

        record_count = (int) (nbytes / 8);
        
        if(TYPE_FIXED_LEN_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)){
            fixedTextLen = header.getInt(HEADER_TYPE_SIZE);
            dis.readFully(text_buffer, 0, (int)nbytes);
        } else {
            fixedTextLen = 0;
            dis.readFully(byteBuffer.array(), 0, (int)nbytes);
        }
        
        if(TYPE_BYTE_ARRAY == header.getInt(HEADER_DATA_TYPE)) {
            record_count = (int) (nbytes);
            int idx = 0;
            for(int i = 0; i < record_count; i++){
                index_buffer[i] = idx;
                idx += byteBuffer.get(i) & 0xFF;
            }
            // Read actual text size                            
            
            dis.readFully(header.array(), 0, header.capacity());                                                
            text_size =  header.getInt(HEADER_DATA_LEN);
            //System.out.format("text_size %d ", text_size);
            dis.readFully(text_buffer, 0, (int)text_size);
        }
        //System.out.format("\n");
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
                if(fixedTextLen > 0){
                    value = new String(text_buffer, fixedTextLen * index, fixedTextLen, StandardCharsets.UTF_8);
                } else {
                    int len = byteBuffer.get(index) & 0xFF;
                    value = new String(text_buffer, index_buffer[index], len, StandardCharsets.UTF_8);
                }
            break;
        }
        return value;
    }
};            
