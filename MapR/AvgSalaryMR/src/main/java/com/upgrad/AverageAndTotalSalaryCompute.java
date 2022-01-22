package com.upgrad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageAndTotalSalaryCompute {

	public static class MapperClass extends Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text empRecord, Context con) throws IOException, InterruptedException {
			String[] word = empRecord.toString().split(" ");
			String gender = word[1];
			Float salary = Float.parseFloat(word[2]);
			con.write(new Text(gender), new FloatWritable(salary));
		}
	}

	public static class ReducerClass extends Reducer<Text, FloatWritable, Text, Text> {

		public void reduce(Text key, Iterable<FloatWritable> valueList, Context con)
				throws IOException, InterruptedException {
			float total = (float) 0;
			int count = 0;
			for (FloatWritable var : valueList) {
				total += var.get();
				count++;
			}
			float avg = total / (float) count;
			String out = "Total: " + total + " :: " + "Average: " + avg;
			con.write(key, new Text(out));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "avgAndTotalSalaryCompute");
		job.setJarByClass(AverageAndTotalSalaryCompute.class);
		job.setMapperClass(MapperClass.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setOutputKeyClass(IntWritable.class);
		// job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
