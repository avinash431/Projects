package com.hadoop.json;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class JsonMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {

		for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
			context.write((Text) entry.getKey(), (Text) entry.getValue());
		}
	}

}
