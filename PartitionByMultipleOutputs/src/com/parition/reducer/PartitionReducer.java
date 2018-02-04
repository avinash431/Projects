package com.parition.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PartitionReducer extends Reducer<Text, Text ,NullWritable, Text> 
{			
	private MultipleOutputs<NullWritable,Text> multipleOutputs;
	public void setup(Context context) throws IOException, InterruptedException
	{
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	public void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws IOException, InterruptedException
	{							
           for(Text value : rvalue) {				              		             		               
          	      multipleOutputs.write(NullWritable.get(), value, "country="+rkey.toString());		          		           }		           
	}											
	public void cleanup(Context context) throws IOException, InterruptedException
	{
	      multipleOutputs.close();
	}		
}	