package com.upgrad;

/*
 * Per state, gender wise rejection ration
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

public class Aadhar1 {

	public static class MapperClass extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text record, Context con) throws IOException, InterruptedException {
			String[] info = record.toString().split(",");
			String state = info[2]; // state will be the key
			String gender = info[6]; // gender will be the value
			con.write(new Text(state), new Text(gender));
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> valueList, Context con) throws IOException, InterruptedException {
			int male_count = 0, female_count = 0;
			String gender;
			for (Text var : valueList) {
				gender = var.toString();
				if (gender.equals("M"))
					male_count++;
				else
					female_count++;
			}
			String out = "Total Male: " + male_count + " :: " + "Total Female: " + female_count;
			con.write(key, new Text(out));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "adhaar1");
		job.setJarByClass(Aadhar1.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/user/cloudera/adhaar_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/output1"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
