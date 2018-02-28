package com.hdfs.avinash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyFileSystem {

	public static void main(String[] args) {

		Configuration conf= new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] filestatus =fs.listStatus(new Path(args[0]));
			
			for (int i = 0; i < filestatus.length; i++) {
				System.out.println(filestatus[i]);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
