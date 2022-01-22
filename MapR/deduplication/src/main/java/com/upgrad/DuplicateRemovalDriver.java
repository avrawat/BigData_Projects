package com.upgrad;
import java.io.*;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;


public class DuplicateRemovalDriver extends Configured implements Tool{
	public int run(String[] args) throws IOException, URISyntaxException{  
		String deltaData = args[0];
		String outputFile = args[1];	
    	Path inputDir1 = new Path(deltaData);
    	Path outputDir = new Path(outputFile);
    	long totalInputRecordCount ;
    	long totalOutputRecordCount ;
    	
   	
    	//common logic
    		int returnCode = 0 ;
    		Job job = Job.getInstance(getConf());       	
        	job.setJobName("Data Validator");        	
        	job.setJarByClass(DuplicateRemovalDriver.class);
        	job.setMapOutputKeyClass(Text.class);
        	job.setMapOutputValueClass(NullWritable.class);               
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(NullWritable.class);      	
        	job.setInputFormatClass(TextInputFormat.class);
        	job.setMapperClass(DuplicateRemovalMapper.class);
        	job.setReducerClass(DuplicateRemovalReducer.class);
        	FileInputFormat.addInputPath(job,inputDir1);  
    		FileSystem fs = FileSystem.get(getConf());
    	
            if (fs.exists(outputDir))
                fs.delete(outputDir, true);
                    
        	FileOutputFormat.setOutputPath(job,outputDir);
        	try {
				returnCode = job.waitForCompletion(true) ? 0 : 1;				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	if(returnCode == 0 ){
        		totalInputRecordCount = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","MAP_INPUT_RECORDS").getValue();
        		totalOutputRecordCount = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue();
        		
            	System.out.println("Total Input Record : " + totalInputRecordCount);
            	System.out.println("Total Output Record : " + totalOutputRecordCount);
            	System.out.println("Job is Success");
            	
            	
        	}

    	return returnCode ;
	}
 
    public static void main(String[] args) throws Exception {
    	
        int returnStatus = ToolRunner.run(new Configuration(), new DuplicateRemovalDriver(), args);
        System.exit(returnStatus);
    }
 
}
