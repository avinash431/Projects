package com.myhadoop.sort.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.myhadoop.sort.driver.Person;

public class SecondarySortMapper extends Mapper<LongWritable, Text, Person , NullWritable> {
	
	private Person p = new Person();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Person, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] line = value.toString().split(" ");
		Text firstName = new Text(line[0]);
		Text lastName = new Text(line[1]);
		p.setFirstName(firstName);
		p.setLastName(lastName);
		context.write(p, NullWritable.get());
		
	}

}
