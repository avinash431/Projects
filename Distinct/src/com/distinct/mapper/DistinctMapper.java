package com.distinct.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] data = line.split("\t");
		context.write(new Text(data[1].toLowerCase()), NullWritable.get());
	}

}
