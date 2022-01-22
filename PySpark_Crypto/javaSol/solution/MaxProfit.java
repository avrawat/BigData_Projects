package solution;

import java.io.Serializable;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import schemaClasses.*;
import scala.Tuple2;

public class MaxProfit implements Serializable {

	// in minute
	private static int windowSize;
	private static int slidingInterval;

	private static final long serialVersionUID = 1L;


	public static void calculateProfit(JavaPairDStream<String, StockPrice> stockStream, String outputpath, int window, int sliding) {
		
		MaxProfit.windowSize = window;
		MaxProfit.slidingInterval = sliding;

		JavaPairDStream<String, PriceData> windowStockDStream = getWindowDStream(stockStream);
		
		JavaPairDStream<Double, String> sortedDSteam = windowStockDStream
				.mapToPair(f -> new Tuple2<String, Double>(f._1.toString(), (f._2.getClose() - f._2.getOpen())))
				.mapValues(t -> new Tuple2<Double, Integer>(t, 1))
				.reduceByKey((t1, t2) -> new Tuple2<Double, Integer>(t1._1() + t2._1(), t1._2() + t2._2()))
				.mapValues(t -> (Double) t._1() / t._2()).mapToPair(f -> new Tuple2<Double, String>(f._2, f._1));

		// Get the stock with maximum profit
		sortedDSteam.foreachRDD(rdd -> {
			Tuple2<Double, String> output = rdd.sortByKey(false).first();
			System.out.println("MAXIMUM PROFIT ANALYSIS: Stock to buy with maximum profit: " + output._2 + ", Profit : "
					+ output._1());
		});

		// Print the stocks and the corresponding profits
		 sortedDSteam.print();
		 System.out.println();

	}

	private static JavaPairDStream<String, PriceData> getWindowDStream(
			JavaPairDStream<String, StockPrice> stockStream) {

		// get only the Price Data of the Stocks
		JavaPairDStream<String, PriceData> stockPriceDStream = getPriceDStream(stockStream);
		

		// take window streams for the stocks
		JavaPairDStream<String, PriceData> windowDStream = stockPriceDStream.window(
				Durations.minutes(windowSize), Durations.minutes(slidingInterval));
		
		// return the combined stream
		return windowDStream;
	}

	private static JavaPairDStream<String, PriceData> getPriceDStream(JavaPairDStream<String, StockPrice> stockStream) {
		// restructure the RDD
		return stockStream.mapToPair(x -> {
			return new Tuple2<String, PriceData>(x._1, x._2.getPriceData());
		});
	}

}