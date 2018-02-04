package com.filter.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line =value.toString();
		String[] data = line.split("\t");
		System.out.printf(data[1]+" " + data[2] + " ");
		System.out.println(data[2].trim().equalsIgnoreCase("Books"));
		if (data[2].trim().equalsIgnoreCase("Books")){
			context.write(NullWritable.get(), value);
		}
	}

}
