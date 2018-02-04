package com.distributed.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Path path = new Path(args[0]);
		BufferedReader br =null;
		try {
			br = new BufferedReader(new FileReader(path.toString()));
			System.out.println(path.getName());
			String line =null;
			while((line = br.readLine())!= null){
				System.out.println(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
