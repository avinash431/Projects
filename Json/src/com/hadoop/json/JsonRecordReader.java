package com.hadoop.json;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonRecordReader extends RecordReader<LongWritable, MapWritable> {

	private final MapWritable value = new MapWritable();
	private final JSONParser jsonParser = new JSONParser();
	
	private LineRecordReader reader = new LineRecordReader();

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	@Override
	public synchronized void close() throws IOException {
		reader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public MapWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (reader.nextKeyValue()) {
			value.clear();
			if (decodeLineToJson(jsonParser, reader.getCurrentValue(), value)) {
				return true;
			}
		}
		return false;
	}

	public boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value) {
		
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
			for (Object key : jsonObj.keySet()) {
				Text mapKey = new Text(key.toString());
				Text mapValue = new Text();
				if (jsonObj.get(key) != null) {
					mapValue.set(jsonObj.get(key).toString());
				}

				value.put(mapKey, mapValue);
			}
			return true;
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return false;
		}
	}
}