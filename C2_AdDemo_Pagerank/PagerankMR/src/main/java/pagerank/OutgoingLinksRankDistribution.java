package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class OutgoingLinksRankDistribution {

	public static class OutgoingLinksMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String currentLine = value.toString();
			String[] split_current_line = currentLine.trim().split("\t");
			String fromID = split_current_line[0];
			String[] transitionIDs = split_current_line[1].split(",");

			if (split_current_line.length == 1) {
				if (split_current_line[1].length() == 0) // dead end
				{
					return;
				}
			}

			for (int i = 0; i < transitionIDs.length; i++) {
				Text temp = new Text(fromID);
				Text prob = new Text(transitionIDs[i] + "=" + ((1.0) / transitionIDs.length));
				context.write(temp, prob);
			}

		}
	}

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String currentLine = value.toString();
			String[] split_current_line = currentLine.trim().split("\t");
			Text temp = new Text(split_current_line[0]);
			Text current_page_rank = new Text(split_current_line[1]);
			context.write(temp, current_page_rank);
		}
	}


	public static class OutgoingLinksReducer extends Reducer<Text, Text, Text, Text> {

		private double beta;

		@Override
		public void setup(Context context) {
			Configuration configuration = context.getConfiguration();
			beta = configuration.getDouble("beta", 0.2);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> transitionIds = new ArrayList<String>();
			double current_page_rank = 0.0;
			//int count = 0;
			for (Text value : values) {
				String val = value.toString();
				if (val.contains("=")) {
					transitionIds.add(val);
				} else {
					transitionIds.add(key.toString()+"=0");
					current_page_rank = Double.parseDouble(val);
				}
			}
			for (int i = 0; i < transitionIds.size(); i++) {
				String temp = transitionIds.get(i);
				String new_key = temp.split("=")[0];
				double value_obtained_from_incoming_link = Double.parseDouble(temp.split("=")[1]);

				double new_val = (1 - beta) * current_page_rank * value_obtained_from_incoming_link;
				Text t1 = new Text(new_key);
				context.write(t1, new Text(new_val + ""));

			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		double beta = 0.2;
		if (args.length > 3) {
			beta = Double.parseDouble(args[3]);
		}
		Configuration configuration = new Configuration();
		configuration.setDouble("beta", beta);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(OutgoingLinksRankDistribution.class);

		ChainMapper.addMapper(job, OutgoingLinksMapper.class, Object.class, Text.class, Text.class, Text.class,
				configuration);
		ChainMapper.addMapper(job, PageRankMapper.class, Object.class, Text.class, Text.class, Text.class,
				configuration);

		job.setReducerClass(OutgoingLinksReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(34);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OutgoingLinksMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageRankMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}

}
