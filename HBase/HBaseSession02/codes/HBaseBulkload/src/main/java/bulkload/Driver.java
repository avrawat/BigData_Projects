package bulkload;

/*
 * This MR Program Bulk loads data from AirLine.csv file present on HDFS to HBase
 * 
 * Takes three input- 
 * 		args[0] input file path (hdfs path)
 * 		args[1] ouput file path (hdfs path, where the hfile will intially be stored)
 * 		args[2] Name of the table to be created.
 * */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		
		if(args.length!=3) {
			System.out.println("Please provide proper inputs");
			return;
		}
		
		// the input file is stored in HDFS
		Path inputPath = new Path(args[0]);
		// outout dir is also in HDFS
		Path outputPath = new Path(args[1]);
		// name of the table we want to create
		String tableName = args[2];
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");
		
		// check if table alredy exists
		if(admin.tableExists(TableName.valueOf(tableName))){
			System.out.println("'"+tableName+"' table name already exists!");
			return;						
		}

		/******************** Creating Table ******************/
		System.out.println("Creating table named " + tableName);
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tableName));
		htable.addFamily(new HColumnDescriptor("time"));
		htable.addFamily(new HColumnDescriptor("flight"));
		htable.addFamily(new HColumnDescriptor("status"));
		admin.createTable(htable);
		System.out.println("table " + tableName + " is Created");
		/******************************************************/

		conf.set("hbase.table.name", tableName);
		
		// Create a job object
		Job job = new Job(conf, "Airline Data Bulk Load");
		
		// set mapper class
		job.setJarByClass(MyMapper.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100);
		
		// Describe table to Map-Reduce job
		Table table = con.getTable(TableName.valueOf(tableName));
		RegionLocator regionLocator = con.getRegionLocator(TableName.valueOf(tableName));
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
		
		// set path variable
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);

		System.out.println("The HFile is created.");

		// Loading data into HBase
		System.out.println("Loading data to HBase...");
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(outputPath, admin, table, regionLocator);
		System.out.println("Driver Terminated");

	}
}
