package demoSpark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class DemoCode {

	public static void main(String[] args) {

		Logger.getRootLogger().setLevel(Level.WARN);
		System.out.println("Please provide- args[0] - Input tableName");
		System.out.println("Please provide- args[1] - output tableName");
		
		if (args.length < 1) {
			System.out.println("Invaild Input");
			return;
		}

		String inputTable = args[0];
		String outputTable = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("HBaseSpark " + inputTable).setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {
			
			Configuration conf = HBaseConfiguration.create();
			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

			Scan scan = new Scan();
			scan.setCaching(500);

			System.out.println("Reading Table....");
			JavaRDD<Tuple2<ImmutableBytesWritable, Result>> rdd = hbaseContext.hbaseRDD(TableName.valueOf(inputTable),
					scan);
			
			// convert to string rdd
			JavaRDD<String> rdd2 = rdd
					.map(x -> Bytes.toString(x._2.getValue(Bytes.toBytes("M"), Bytes.toBytes("year"))));
			
			// convert to key-value pair
			JavaPairRDD<String, Integer> pairRdd01 = rdd2.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
			
			// count the keys
			JavaPairRDD<String, Integer> countRDD = pairRdd01.reduceByKey((x, y) -> x + y);
			
			// get the string rdd
			JavaRDD<String> output = countRDD.map(x -> x._1 + "," + x._2);
			
			//save the results back to a HBase table
			hbaseContext.bulkPut(output, TableName.valueOf(outputTable), new PutFunction());

			System.out.println("Result Size: " + output.count());
			System.out.println("Printing data...");
			for (String result : output.collect()) {
				System.out.println(result);
			}

		} finally {
			jsc.stop();
		}

		System.out.println("Code Terminated");
	}

	static class PutFunction implements Function<String, Put> {

		private static final long serialVersionUID = 1L;
		public Put call(String v) throws Exception {
			String[] cells = v.split(",");
			Put put = new Put(Bytes.toBytes(cells[0]));
			put.addColumn(Bytes.toBytes("C"), Bytes.toBytes("Count"), Bytes.toBytes(cells[1]));
			return put;
		}

	}

}
