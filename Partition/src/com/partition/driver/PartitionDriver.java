package com.partition.driver;

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

import com.partition.mapper.PartitionMapper;
import com.partition.reducer.PartitionReducer;

public class PartitionDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Parition");
		job.setJarByClass(PartitionDriver.class);
		
		job.setMapperClass(PartitionMapper.class);
	    job.setReducerClass(PartitionReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		FileSystem fs = FileSystem.get(conf);
		
		if (fs.exists(output))
		{
			fs.delete(output, true);
		}
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			int exitCode = ToolRunner.run(new PartitionDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
