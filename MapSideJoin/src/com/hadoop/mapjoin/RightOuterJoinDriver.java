package com.hadoop.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RightOuterJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(RightOuterJoinDriver.class);
		job.setJobName("Map Right Outer Side Join");
		
		job.setMapperClass(RightOuterJoinMapper.class);
		job.setNumReduceTasks(0);
		
		Path customerinput = new Path(arg0[0]);
		Path orderinput = new Path(arg0[1]);
		Path output = new Path(arg0[2]);
		
		job.addCacheFile(customerinput.toUri());
		
		FileInputFormat.addInputPath(job, orderinput);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)? 0 :1 ;
	}

	public static void main(String[] args) {

		try {
			int res = ToolRunner.run(new RightOuterJoinDriver(), args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
