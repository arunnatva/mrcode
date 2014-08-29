
/* Hi, I have copied below jars into classpath in order to import the required packages, classes.

hadoop-core.jar
commons-logging-1.1.1.jar
hadoop-common-2.0.0-cdhx.x.jar
commons-configuration-1.6.jar
hadoop-core-mr1-cdhx.x.jar

package com.arun.mrcode;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FSDataInputStream;

class MultiLineRecordReader extends RecordReader <LongWritable, Text> {

 private LongWritable key = null;
 private Text value = new Text();
 private LineReader lr;
 private long start;
 private long end;
 private long pos;
 private int mllength;
 
 @Override
 public void initalize (InputSplit insplit, TaskAttemptContext tas) throws IOException, InterruptedException {
 
       FileSplit fsplit  = (FileSplit) Insplit;
       final Path fpath = fsplit.getPath();
       Configuration conf = tas.getConfiguration();
       
 
 
