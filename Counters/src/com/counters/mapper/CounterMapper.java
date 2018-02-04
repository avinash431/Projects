package com.counters.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.counters.enums.ErrorCount;

public class CounterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		 if (line.split(",").length != 15){
			 System.out.println(line);
			 context.getCounter(ErrorCount.errorRecords).increment(1);
			 return;
		 }
		 if (context.getCounter(ErrorCount.errorRecords).getValue() >= 5){
			 System.out.println("Error records exceeded more than 5 hence terminating");
			 System.exit(1);
		 }
		 context.write(key, value);
	}

}
