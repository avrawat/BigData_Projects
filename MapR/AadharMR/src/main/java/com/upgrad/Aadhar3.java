package com.upgrad;

/*
 * MR-01 state wise total rejection ratio
 * MR-02 average value to the output of MR-01
 * 
 * */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Aadhar3 {

	public static class MapperClass extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text record, Context con) throws IOException, InterruptedException {
			String[] info = record.toString().split(",");
			String state = info[2];
			String rejected = info[9];
			con.write(new Text(state), new Text(rejected));
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> valueList, Context con) throws IOException, InterruptedException {
			int rejected_count = 0;
			String rejected;
			for (Text var : valueList) {
				rejected = var.toString();
				if (rejected.equals("1"))
					rejected_count++;
			}
			String out = "::" + rejected_count;
			con.write(key, new Text(out));
		}
	}

	public static class MapperClassSort extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text record, Context con) throws IOException, InterruptedException {
			String rejections = record.toString().trim().split("::")[1];
			con.write(new Text("1"), new Text(rejections));
		}
	}

	public static class ReducerClassSort extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> valueList, Context con) throws IOException, InterruptedException {
			int total = 0, count = 0;
			for (Text val : valueList) {
				total += Integer.parseInt(val.toString());
				count++;
			}
			double avg = total / (double) count;
			String op = "Average in all states is " + avg;
			con.write(new Text(""), new Text(op));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf;
		Job job;

		conf = new Configuration();

		job = Job.getInstance(conf, "adhaar3_a");
		job.setJarByClass(Aadhar3.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/cloudera/adhaar_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/op8Temp/"));

		job.waitForCompletion(true);

		// conf = new Configuration();

		job = Job.getInstance(conf, "adhaar3_b");
		job.setJarByClass(Aadhar3.class);

		job.setMapperClass(MapperClassSort.class);
		job.setReducerClass(ReducerClassSort.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/cloudera/op8Temp/"));
		FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/op8Final/"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
