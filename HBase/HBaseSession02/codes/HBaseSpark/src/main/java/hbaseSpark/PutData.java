package hbaseSpark;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class PutData {

	public static void main(String[] args) throws IOException {

		Logger.getRootLogger().setLevel(Level.WARN);

		if (args.length < 2) {
			System.out.println("invaild input");
			System.exit(0);
		}
		
		// path of the data file
		String path = args[0];
		// table name
		String tableName = args[1];
		
		// spark configuration object
		SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkPutExample " + tableName).setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		
		// java saprk context
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {
			//hbase configuration
			Configuration config = HBaseConfiguration.create();
			
			/******************************************* Create table ****************************************************/
			
			TableDescriptorBuilder descBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("M")).build());
			TableDescriptor tableDesc = descBuilder.build();
			Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
			conn.getAdmin().createTable(tableDesc);
			/*****************************************************************************************************/
			
			System.out.println(tableName + " table is created");
			
			// read the text file in a string RDD, line by line
			JavaRDD<String> input = jsc.textFile(path);
			
			// java hbase context, class acts the bridge between,HBase and Spark
			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, config);
			
			// call the bulk put method
			hbaseContext.bulkPut(input, TableName.valueOf(tableName), new PutFunction());

		} finally {
			jsc.stop();
		}

		System.out.println("Code terminated!");
	}

	static class PutFunction implements Function<String, Put> {

		private static final long serialVersionUID = 1L;
		
		// split the cs row, and out the value to hbase table using the Put API
		public Put call(String v) throws Exception {
			String[] cells = v.split(";");
			
			/* the movies table has following structure
			   id,movie name, year 
			*/	
			
			Put put = new Put(Bytes.toBytes(cells[0]));
			put.addColumn(Bytes.toBytes("M"), Bytes.toBytes("name"), Bytes.toBytes(cells[1]));
			put.addColumn(Bytes.toBytes("M"), Bytes.toBytes("year"), Bytes.toBytes(cells[2]));
			return put;
			
		}

	}

}
