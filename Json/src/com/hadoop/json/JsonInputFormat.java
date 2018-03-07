package com.hadoop.json;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class JsonInputFormat extends FileInputFormat<LongWritable, MapWritable> {
	
	protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }
	
	 public RecordReader<LongWritable, MapWritable>  createRecordReader(InputSplit split,TaskAttemptContext context) {
		    return new JsonRecordReader();
		  }

}
