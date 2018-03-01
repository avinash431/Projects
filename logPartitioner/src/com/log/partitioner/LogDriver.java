package com.log.partitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class LogDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(LogDriver.class);
		job.setJobName("Partition");
		
		job.setMapperClass(LogMapper.class);
		job.setNumReduceTasks(0);
		
		Path input = new Path(arg0[0]);
		Path output = new Path(arg0[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		return job.waitForCompletion(true)? 0 :1 ;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int res = ToolRunner.run(new LogDriver(), args);
			if(res ==0){
				LogDriver lg = new LogDriver();
				FileSystem fs = FileSystem.get(lg.getConf());
				Path output = new Path(args[1]);
				Path dest = new Path(args[3]);
				if(fs.exists(output)){
					String success_file_path = output.getName()+"_SUCCESS";
					System.out.println("Suucess file path is "+success_file_path);
					fs.delete(new Path(success_file_path),true);
					FileStatus[] status = fs.globStatus(output);
					Path[] paths = FileUtil.stat2Paths(status);
					for (int i = 0; i < paths.length; i++) {
						String temp = dest.toString()+"/"+paths[i].getName();
						fs.rename(paths[i], new Path(temp));
						
					}
					
					
				}
			}
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
