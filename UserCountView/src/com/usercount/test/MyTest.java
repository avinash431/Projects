package com.usercount.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class MyTest extends Configured {
	
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		System.out.println(conf.get("fs.defaultFS"));
	}

}
