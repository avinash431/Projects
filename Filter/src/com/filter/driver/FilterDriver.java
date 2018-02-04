package com.filter.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.filter.mapper.FilterMapper;

public class FilterDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FilterMapper.class);
		
		job.setNumReduceTasks(0);
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
	
		
		return job.waitForCompletion(true) ? 0 : 1 ;
		
	}

	public static void main(String[] args) {
		int exitCode;
		try {
			exitCode = ToolRunner.run(new FilterDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
