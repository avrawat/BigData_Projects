package pagerank;

/*
 * PageRank Spark Implementation
 * 
 * */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Iterables;
import scala.Tuple2;

public class rddPagerank {

	// number of iterations
	private static int itrh;

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		Logger.getLogger("org").setLevel(Level.ERROR);

		// ------------------------------------------------------------------------------------------------------------------------------
		
		String pr = args[0];
		String network = args[1];
		String outpath = args[2];
		itrh = Integer.parseInt(args[3]);

		SparkConf sparkConf = new SparkConf().setAppName("Spark_PageRank").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		
		 /*
		  // local mode
		  
		  SparkConf sparkConf = new SparkConf().setAppName("Spark_PageRank_RDD1").setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		  JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		  String network = "dataset/network.txt"; 
		  String pr = "dataset/pr.txt"; 
		  String outpath = "results/local"; 
		  itrh = 2; 
		*/
		
		System.out.println("Reading the files...");
		// page rank rdd
		JavaRDD<String> plines = ctx.textFile(pr, 1);
		// network rdd
		JavaRDD<String> lines = ctx.textFile(network, 1);

		// page rank pair rdd
		JavaPairRDD<String, Double> pagerank = plines.mapToPair(new PairFunction<String, String, Double>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Double> call(String t) throws Exception {

				String[] parts = t.split("\t");

				Double d = Double.parseDouble(parts[1]);

				return new Tuple2<String, Double>(parts[0], d);
			}
		});

		// network pair rdd
		JavaPairRDD<String, Iterable<String>> links = lines
				.mapToPair(new PairFunction<String, String, Iterable<String>>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Iterable<String>> call(String s) {
						String[] parts = s.split("\t");

						String[] links = parts[1].split(",");
						List<String> nodes = new ArrayList<String>();

						for (String link : links) {
							nodes.add(link);
						}

						return new Tuple2<String, Iterable<String>>(parts[0], nodes);
					}
				});

		// (1-d)/N
		Double d = (1 / (double) pagerank.count()) * 0.2;

		Broadcast<Double> init = ctx.broadcast(d);

		//System.out.println("value " + init.value() + " " + pagerank.count());

		links = links.partitionBy(new CustomPartitioner(17));

		for (int current = 0; current < itrh; current++) {

			System.out.println("********** Iteration--> " + (current + 1) + " **********");
			// get the shared page rank
			JavaPairRDD<String, Double> sPR = links.join(pagerank).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {

						private static final long serialVersionUID = 1L;

						public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
							// number of out links
							int urlCount = Iterables.size(s._1);

							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							for (String n : s._1) {

								results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
							}
							return results.iterator();
						}
					}).reduceByKey((x, y) -> x + y);

			pagerank = pagerank.leftOuterJoin(sPR)
					.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Optional<Double>>>, String, Double>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Optional<Double>>> t)
								throws Exception {

							if (!t._2._2.isPresent()) {
								
								Double d = init.value();
								return new Tuple2<String, Double>(t._1, d);
							}

							Double d = init.value() + t._2._2.get() * 0.8;
							return new Tuple2<String, Double>(t._1, d);
						}

					});

		} // page rank iterations

		// not required, remove from the memory
		links.unpersist();
		// will be storing it
		pagerank.cache();

		System.out.println("********** Final Output **********");
		List<Tuple2<String, Double>> output = pagerank.take(10);
		for (Tuple2<String, Double> tuple : output) {
			System.out.println(tuple._1() + " " + tuple._2() + ".");
		}

		long total = System.currentTimeMillis() - start;
		System.out.println("Computaion time taken: " + total / 60000 + " mins");

		System.out.println("Saving results...");
		pagerank.saveAsTextFile(outpath + "RDD_PageRanks");

		// closing spark context
		ctx.close();
		total = System.currentTimeMillis() - start;
		System.out.println("total time taken: " + total / 60000 + " mins");
	}

}
