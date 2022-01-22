package hbaseMR;

/*
 * 
 * This MR Program calcultes year wise average delay on the airline data
 * 
 * Driver.java - Main Class
 * 	> Takes two command line input
 * 		> args[0] -  source table name            
 *      > args[1] - destination table name
 * */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// calling run method
		
		Logger.getRootLogger().setLevel(Level.INFO);
		
		int returnStatus = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(returnStatus);
	}

	public int run(String[] args) throws IOException {

		// get HBase configutation
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();

		// define job ojectConnection
		Job job = new Job(conf, "ExampleSummary");
		job.setJarByClass(Driver.class);

		// source table name
		String sourceTable = args[0];

		// need to created first
		String targetTable = args[1];

		// check if table Name already exists
		if (!admin.tableExists(TableName.valueOf(sourceTable))) {
			System.out.println("The  Source table '" + sourceTable + "' does not Exists!");
			System.exit(0);
		}

		// check if table Name already exists
		if (admin.tableExists(TableName.valueOf(targetTable))) {
			System.out.println("The  target table name '" + targetTable + "' already Exists!");
			System.exit(0);
		}

		/******************** Creating Table ******************/
		System.out.println("Creating table named " + targetTable);
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(targetTable));
		htable.addFamily(new HColumnDescriptor("Delay"));
		admin.createTable(htable);
		System.out.println("table '" + targetTable + "' is Created");
		/******************************************************/
		
		// for reading the table
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(sourceTable, scan, MyMapper.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(targetTable, MyReducer.class, job);
		job.setNumReduceTasks(1); // by default it is one, incase of large cluster increse it

		try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;

	}
}