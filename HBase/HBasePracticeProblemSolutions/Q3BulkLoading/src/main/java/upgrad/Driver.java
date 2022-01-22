package upgrad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		// avoid while debugging
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		String tableName = args[2];

		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");

		
		if(admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("The table named '"+tableName+"' already exists.");
			return;
		}
		
		/******************** Creating Table ******************/

		System.out.println("Creating table named " + tableName);
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tableName));
		htable.addFamily(new HColumnDescriptor("M"));
		admin.createTable(htable);
		System.out.println("table " + tableName + " is Created");
		/******************************************************/

		conf.set("hbase.table.name", tableName);
		Job job = new Job(conf, "Movies_Data_Bulk_Load");

		job.setJarByClass(Driver.class); // point break
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100);
		
		System.out.println("Getting Table object...");
		Table table = con.getTable(TableName.valueOf(tableName));
		System.out.println("Getting RegionLoacator object...");
		RegionLocator regionLocator = con.getRegionLocator(TableName.valueOf(tableName));

		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Logger.getRootLogger().setLevel(Level.INFO);
		System.out.println("Starting Map task...");
		job.waitForCompletion(true);
		Logger.getRootLogger().setLevel(Level.WARN);
		System.out.println("Map task finished...");
		System.out.println("The HFiles are created.");

		// Loading data into HBase
		System.out.println("Loading HFiles to HBase...");
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(outputPath, admin, table, regionLocator);
		System.out.println("Table '"+ tableName +"' is Bulk Loaded.");
		System.out.println("Driver Terminated");

	}

}