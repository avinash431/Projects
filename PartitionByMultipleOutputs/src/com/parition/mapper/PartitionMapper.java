package com.parition.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONObject;

public class PartitionMapper extends Mapper<LongWritable, Text, Text, Text> 
	{			
		public void map(LongWritable mkey, Text mvalue, Context context) throws IOException, InterruptedException
             {
           		    try
                   {
	               JSONObject jsonObj = new JSONObject(mvalue.toString());
		               //parse the input data with JSONObject	
	               String country = (String)jsonObj.get("country");
	               String state = (String)jsonObj.get("state");
	               String city = (String)jsonObj.get("city");
	               String street = (String)jsonObj.get("street");
	               String zip = (String)jsonObj.get("zip");
	               
	               StringBuilder key = new StringBuilder();
	               key.append(country);
	               key.append("/");
	               key.append(state);
	               key.append("/");
	               key.append(city);
	               key.append("/");
	               key.append(street);
	               key.append("/");
	               key.append(zip);
	               //emitting directory structure as key and input record as value.
	               context.write(new Text(key.toString()), mvalue);	          			    
                    }
                    catch (Exception e) {
				e.printStackTrace();
			}
		     }
		}