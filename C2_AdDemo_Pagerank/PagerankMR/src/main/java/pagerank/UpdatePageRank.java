package pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UpdatePageRank {

	public static class IncomingLinksMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String currentLine = value.toString();
			String[] split_current_line = currentLine.trim().split("\t");
			Text id = new Text(split_current_line[0]);
			context.write(id, new DoubleWritable(Double.parseDouble(split_current_line[1])));
		}
	}


	public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			sum += 0.2*1/1000000; // (1-d)/n term

			
			context.write(key, new DoubleWritable(sum));
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
		job.setJarByClass(UpdatePageRank.class);
				
		
		job.setMapperClass(IncomingLinksMapper.class);
		job.setReducerClass(SumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, IncomingLinksMapper.class);
		

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}

}
