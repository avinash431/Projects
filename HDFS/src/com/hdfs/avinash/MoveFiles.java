package com.hdfs.avinash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class MoveFiles extends Configured {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//MoveFiles mv = new MoveFiles();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(args[0]);
		Path dest = new Path(args[1]);
		if(fs.exists(output)){
			//String success_file_path = output.toString()+"/"+"_SUCCESS";
			//System.out.println("Suucess file path is "+success_file_path);
			//fs.delete(new Path(success_file_path),true);
			FileStatus[] filestatus = fs.listStatus(output);
			for (int i = 0; i < filestatus.length; i++) {
				String path =filestatus[i].getPath().getName();
				Path file_path = new Path(path);
				System.out.println("File path is "+path);
				Path dest_path = new Path(dest+"/"+path);
				System.out.println("Destination path is "+dest_path);
				if(fs.isDirectory(file_path)){
					String tmp_dir = output.toString()+"/"+file_path.getName();
					System.out.println("Creating the tmp dir "+ tmp_dir);
					fs.mkdirs(new Path(tmp_dir));				
				}
				else{
					fs.rename(new Path(path), dest_path);
				}
				
			}
		
				
			

	}

	}
}
