package com.upgrad;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DuplicateRemovalReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	
	public void setup(Context context) throws IOException {
		
		
	}
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
						

		    context.write(key,NullWritable.get());  
		    

	}
    public void cleanup(Context context) throws IOException,
	InterruptedException {
    	

}
}
