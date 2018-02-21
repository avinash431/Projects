package com.myhadoop.sort.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.myhadoop.sort.driver.Person;

public class SecondarySortReducer extends Reducer<Person, NullWritable, Text, Text> {
	
	@Override
	protected void reduce(Person p, Iterable<NullWritable> arg1,
			Reducer<Person, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Text firstName = p.getFirstName();
		Text lastName = p.getLastName();
		context.write(firstName, lastName);
		
	}

}
