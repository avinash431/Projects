package com.myhadoop.sort.comparator;

import org.apache.hadoop.io.WritableComparator;

public class PersonComparator extends WritableComparator {
	
	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		// TODO Auto-generated method stub
		return super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
	}

}
