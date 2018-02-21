package com.myhadoop.sort.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.myhadoop.sort.driver.Person;

public class PersonPartitioner extends Partitioner<Person, NullWritable> {

	@Override
	public int getPartition(Person key, NullWritable value, int numPartitions) {
		
		
		return Math.abs((key.getFirstName().hashCode()*127)%numPartitions);
	}

}
