package com.myhadoop.sort.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.myhadoop.sort.mapper.SecondarySortMapper;
import com.myhadoop.sort.partitioner.PersonPartitioner;
import com.myhadoop.sort.reducer.SecondarySortReducer;

public class SecondarySortDriver extends Configured implements Tool {
	
	private static Job job;
	private static SecondarySortDriver mydriver = new SecondarySortDriver();
	private static Configuration conf = new Configuration();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			for (int i = 0; i < args.length; i++) {
				System.out.println(args[i]);
				
			}
			job = parseInputAndOutput(mydriver,conf, args);			
			if(job != null){				
				
				System.out.println("Running the job");
				int exitCode = ToolRunner.run(mydriver, args);
			    System.exit(exitCode);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		job = setJobParameters(job);
		return job.waitForCompletion(true)? 0:1;
	}

private static Job setJobParameters(Job job) {
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(PersonPartitioner.class);
		
		
		job.getConfiguration().set("mapreduce.job.ubertask.enable", "true");	
		job.getConfiguration().set("mapreduce.job.ubertask.maxbytes", "4096");
		job.getConfiguration().set("mapreduce.job.ubertask.maxmaps","9");
		job.getConfiguration().set("mapreduce.job.ubertask.maxreduces","1");
		return job;
	}


	public static Job parseInputAndOutput(Tool tool, Configuration conf,
		      String[] args) throws IOException {
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args); 
		
		 String[] otherArgs = parser.getRemainingArgs();
		 System.out.println("Other args are ");
		 for (int i = 0; i < otherArgs.length; i++) {
			 System.out.println(otherArgs[i]);
			
		}
		 if (otherArgs.length < 2) {
		      printUsage(tool, "<input> <output>");
		      return null;
		    }
		 
		 //  conf.set(name, value);
		 
		    Job job = Job.getInstance(conf);
		    job.setJarByClass(tool.getClass());
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    return job;
		  }

	 public static void printUsage(Tool tool, String extraArgsUsage) {
		    System.err.printf("Usage: %s [genericOptions] %s\n\n",
		        tool.getClass().getSimpleName(), extraArgsUsage);
		    GenericOptionsParser.printGenericCommandUsage(System.err);
		  }
}
