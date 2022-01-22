package com.upgrad;

/*
 * MR-01 State wise total rejection ratio
 * MR-02 Sorting output of the MR-01 in descending order.
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

public class Aadhar2 {

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
			con.write(new Text("1"), record); // we are using the same key so that all the values reaches the same
												// reducer
		}
	}

	public static class ReducerClassSort extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> valueList, Context con) throws IOException, InterruptedException {

			String[][] output = new String[100][2];
			String[] inp;
			String state;
			int pop, i, j;

			i = 0;

			for (Text var : valueList) {

				inp = var.toString().split("::");
				state = inp[0].trim();
				pop = Integer.parseInt(inp[1].trim());

				if (i == 0) {
					output[i][0] = state;
					output[i][1] = pop + "";
				} else {
					/*
					 * Insertion sort is used
					 */

					for (j = i - 1; j >= 0; j--) {
						if (Integer.parseInt(output[j][1]) > pop) {
							output[j + 1][0] = output[j][0];
							output[j + 1][1] = output[j][1];
						} else
							break;
					}
					output[j + 1][0] = state;
					output[j + 1][1] = pop + "";
				}

				i++;

			}

			String out = "";

			for (j = i - 1; j >= 0; j--)
				out = out + output[j][0] + " " + output[j][1] + "\n";

			con.write(new Text(""), new Text(out));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf;
		Job job;

		conf = new Configuration();

		job = Job.getInstance(conf, "adhaar1_a");
		job.setJarByClass(Aadhar2.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/cloudera/adhaar_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/opTemp24/"));

		job.waitForCompletion(true); // this is for the first job to complete

		// conf = new Configuration();

		job = Job.getInstance(conf, "adhaar1_b");
		job.setJarByClass(Aadhar2.class);

		job.setMapperClass(MapperClassSort.class);
		job.setReducerClass(ReducerClassSort.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/cloudera/opTemp24/"));
		FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/Finalout1/"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
