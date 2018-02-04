package com.usercount.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.usercount.Reducer.UserCountReducer;
import com.usercount.mapper.UserCountMapper;

public class UserCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.get("fs.defaultFS");
		Job job = Job.getInstance(conf);
		job.setJobName("UserCountView");
		job.setJarByClass(UserCountDriver.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(UserCountMapper.class);
		job.setReducerClass(UserCountReducer.class);
		
		Path input = new Path(arg[0]);
		Path output = new Path(arg[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) {
		
		try {
			int returnCode = ToolRunner.run(new UserCountDriver(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
