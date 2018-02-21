package com.myhadoop.sort.driver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable<Person> {
	
	private Text firstName;
	private Text lastName;
	
	public Text getFirstName() {
		return firstName;
	}
	public void setFirstName(Text firstName) {
		this.firstName = firstName;
	}
	public Text getLastName() {
		return lastName;
	}
	public void setLastName(Text lastName) {
		this.lastName = lastName;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.firstName = new Text(in.readUTF());
		this.lastName=new Text(in.readUTF());
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.firstName.toString());
		out.writeUTF(this.lastName.toString());
		
	}
	@Override
	public int compareTo(Person o) {
		// TODO Auto-generated method stub
		int cmp = this.firstName.compareTo(o.getFirstName());
		if(0 != cmp){
			return cmp;
		}
		return this.lastName.compareTo(o.getLastName());
	}

}
