package com.parition.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.parition.mapper.PartitionMapper;
import com.parition.reducer.PartitionReducer;

public class PartitionDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();		
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);

		Job job = Job.getInstance(conf);		
		job.setJarByClass(PartitionDriver.class);
		
		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(PartitionReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		;
		  
		job.getConfiguration().set("mapred.child.java.opts","-Xmx2048m");
		job.getConfiguration().setInt("mapreduce.map.memory.mb",2048);
		job.getConfiguration().setInt("mapreduce.reduce.memory.mb",2048);
		job.getConfiguration().set("mapreduce.map.java.opts","-Xmx2048m");
		job.getConfiguration().set("mapreduce.reduce.java.opts","-Xmx2048m");
		job.getConfiguration().setBoolean("mapreduce.reduce.speculative", true);

		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
				
		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}
		
		return job.waitForCompletion(true)? 0:1 ;	
			   
	}
	public static void main(String[] args) throws Exception 
	{
		int exitCode = ToolRunner.run(new PartitionDriver(), args);
		    System.exit(exitCode);
        }
	}


