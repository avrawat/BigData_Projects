package com.upgrad;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class DuplicateRemovalMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	
	public void setup(Context context) throws IOException {
	
		
	}
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    context.write(value, NullWritable.get());  
		    

	}
    public void cleanup(Context context) throws IOException,
	InterruptedException {
    	

}
}
