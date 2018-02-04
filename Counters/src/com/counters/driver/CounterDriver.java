package com.counters.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.counters.mapper.CounterMapper;

public class CounterDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = this.getConf();
		Job job =Job.getInstance(conf);
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(CounterMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			int exitCode = ToolRunner.run(new CounterDriver(), args);
			System.out.println("Exit code is "+exitCode );
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
