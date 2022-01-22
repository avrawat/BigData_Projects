package com.upgrad.mapjoin;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import org.apache.hadoop.conf.*;

public class MapSideJoinDriver extends Configured implements Tool {
	public int run(String[] args) throws IOException, URISyntaxException {

		long totalRecordCount;
		long errorRecordCount = 0;

		// common logic
		int returnCode = 0;
		Job job = new Job(getConf());
		job.setJobName("Data Validator");
		job.setJarByClass(MapSideJoinDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setNumReduceTasks(0);

		FileSystem fs = FileSystem.get(getConf());
		job.addCacheFile(new Path(args[1]).toUri());// full file path with name

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		try {
			returnCode = job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (returnCode == 0) {
			totalRecordCount = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
		}
		if (errorRecordCount > 0) {
			System.out.println("Job failed as there are " + errorRecordCount + " error records");
		} else {
			System.out.println("Job is Success. No Error Records found");
		}
		return returnCode;
	}

	public static void main(String[] args) throws Exception {

		int returnStatus = ToolRunner.run(new Configuration(), new MapSideJoinDriver(), args);
		System.exit(returnStatus);
	}

}
