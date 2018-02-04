package com.partition.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PartitionMapper extends  Mapper<LongWritable, Text, Text, Text> {
	
	/*private MultipleOutputs<NullWritable, Text> mults;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
mults = new MultipleOutputs<NullWritable, Text>(context);
	}
		*/
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String date = line.split(",")[0];
		date = date.replace("-", "/");
		System.out.println("Date isssssssssss "+date);
		//mults.write(NullWritable.get(), value, date);
		context.write(new Text(date), value);
	}
	
	/*@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mults.close();
	}*/

}
