package pagerank;

/*
 * PageRank- Spark Dataset implementation
 * 
 * */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class datasetPagerank {

	// number of iterations
	private static int itrh;

	public static void main(String[] args) {

		long start = System.currentTimeMillis();

		Logger.getLogger("org").setLevel(Level.ERROR);
		// ------------------------------------------------------------------------------------------------------------------------------

		String prPath = args[0];
		String netPath = args[1];
		String outputPath = args[2];
		itrh = Integer.parseInt(args[3]);

		SparkSession session = SparkSession.builder().appName("Spark_PageRank_DataSet_" + itrh)
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();

		/*
		 * //local mode
		 * //---------------------------------------------------------------------------
		 * ---------------------------------------------------
		 * 
		 * SparkSession session =
		 * SparkSession.builder().appName("Spark_PageRank_DataSet").master("local[2]")
		 * .config("spark.serializer",
		 * "org.apache.spark.serializer.KryoSerializer").getOrCreate();
		 * 
		 * String prPath = "dataset/pr.txt"; String netPath = "dataset/network.txt";
		 * String outputPath = "results/local"; itrh = 2;
		 * 
		 * //---------------------------------------------------------------------------
		 * --------------------------------------------------- //
		 */

		// schema for the data sets
		StructType PRschema = new StructType().add("node", DataTypes.IntegerType).add("pr", DataTypes.DoubleType);
		StructType NETschema = new StructType().add("node", DataTypes.IntegerType).add("links", DataTypes.StringType);

		// read the data sets
		Dataset<Row> pagerank = session.read().option("delimiter", "\t").format("com.databricks.spark.csv")
				.schema(PRschema).load(prPath);
		Dataset<Row> network = session.read().option("delimiter", "\t").format("com.databricks.spark.csv")
				.schema(NETschema).load(netPath);

		System.out.println("=== Print out schema ratings ===");
		pagerank.printSchema();
		network.printSchema();

		pagerank.limit(10).show();
		network.limit(10).show();

		// join the data sets
		Dataset<Row> data = pagerank.join(network, pagerank.col("node").equalTo(network.col("node")))
				.select(pagerank.col("node"), pagerank.col("pr"), network.col("links"));

		// (1-d) / N
		double val = (1 / (double) pagerank.count()) * 0.2;
		pagerank.unpersist();
		network.unpersist();

		System.out.println("****************************************************");

		// schema to store intermediate data.
		StructType structType = new StructType().add("node", DataTypes.StringType).add("pr1", DataTypes.DoubleType);

		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

		// page rank iterations
		for (int itr = 0; itr < itrh; itr++) {

			System.out.println("Iteration Number--> " + (itr + 1) + " of " + itrh);

			// get the shared page rank
			Dataset<Row> spr = data.flatMap(new FlatMapFunction<Row, Row>() {
				private static final long serialVersionUID = 1L;

				public Iterator<Row> call(Row row) throws Exception {

					String[] links = row.getString(2).split(",");
					// number of out links
					int nol = links.length;

					List<Row> results = new ArrayList<Row>(nol);

					for (String link : links) {
						// 0.8 is the damping factor
						double npr = 0.8 * (row.getDouble(1) / nol);
						results.add(RowFactory.create(link, npr));
					}
					return results.iterator();
				}
			}, encoder);

			// sum all the page ranks coming to a node (reduce by key)
			String q1 = "select node, sum(pr1) as pr1 from tempView group by node";
			spr.createOrReplaceTempView("tempView");
			spr = session.sql(q1);

			// join the intermediate data
			data = data.join(spr, data.col("node").equalTo(spr.col("node")), "full_outer").select(data.col("node"),
					data.col("pr"), data.col("links"), spr.col("pr1"));

			// filter out the null values
			data = data.select("*").filter("node!=0");
			data = data.na().fill(0);

			// 0.2 is (1-d) damping factor
			// update the page rank of this iteration

			String q2 = String.format("select node, %8f+pr1 as pr, links from data2", val);
			data.createOrReplaceTempView("data2");
			data = session.sql(q2);

		} // for loop

		System.out.println("****************************************************");

		data.limit(10).show();

		long total = System.currentTimeMillis() - start;
		System.out.println("Computaion time taken: " + total / 60000 + " mins");

		System.out.println("Storing results...");

		data.select("node", "pr").cache().write().format("csv")
				.save(outputPath + "DF_PageRank" + (System.currentTimeMillis() / 1000));

		session.close();

		total = System.currentTimeMillis() - start;
		System.out.println("total time taken: " + total / 60000 + " mins");

	}// main
}// main class
