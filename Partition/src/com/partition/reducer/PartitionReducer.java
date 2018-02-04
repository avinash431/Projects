package com.partition.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PartitionReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mults;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mults = new MultipleOutputs<NullWritable, Text>(context);	
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		for (Text text : values) {
			mults.write(NullWritable.get(), text, "country="+key.toString());
			
		}
		
	}
	
	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	   mults.close();
	}
}


