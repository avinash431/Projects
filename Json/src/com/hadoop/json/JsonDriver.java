package com.hadoop.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JsonDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub

		Job job = Job.getInstance(getConf());
		job.setJarByClass(JsonDriver.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(JsonMapper.class);
		
		job.setInputFormatClass(JsonInputFormat.class);
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		Path input = new Path(arg0[0]);
		Path output = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);		

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JsonDriver(), args);
		System.exit(res);
	}

}
