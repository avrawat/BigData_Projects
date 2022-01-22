package solution;

import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import schemaClasses.*;

import scala.Tuple2;

public class VolumeStocks implements Serializable {
	// in minute
	private static int windowSize;
	private static int slidingInterval;
	
	private static final long serialVersionUID = 1L;

	// summary function
	private static Function2<PriceData, PriceData, PriceData> SUM_REDUCER_PRICE_DATA_VOLUME = (a, b) -> {
		a.setVolume(a.getVolume() + b.getVolume());
		return a;
	};

	// inverse function
	private static Function2<PriceData, PriceData, PriceData> DIFF_REDUCER_PRICE_DATA_VOLUME = (a, b) -> {
		a.setVolume(a.getVolume() - b.getVolume());
		return a;
	};

	public static void calculateVolume(JavaPairDStream<String, StockPrice> stockStream, String outputpath, int window, int sliding)
			throws IOException {
		
		VolumeStocks.windowSize = window;
		VolumeStocks.slidingInterval = sliding;
		
	//	stockStream.print();
		
		JavaPairDStream<String, PriceData> windowStockDStream = getWindowDStream(stockStream);
		
		

		JavaPairDStream<Double, String> sortedDSteam = windowStockDStream
				.mapToPair(f -> new Tuple2<Double, String>(f._2.getVolume(), f._1.toString()));

		// Get the stock with maximum volume
		sortedDSteam.foreachRDD(rdd -> {
			Tuple2<Double, String> output = rdd.sortByKey(false).first();

			System.out.println(
					"VOLUME ANALYSIS: Stock to buy with maximum volume: " + output._2 + ", Volume : " + output._1());
		});

		// Print the stocks and the corresponding volumes
		sortedDSteam.print();
		System.out.println();

	}

	private static JavaPairDStream<String, PriceData> getWindowDStream(
			JavaPairDStream<String, StockPrice> stockStream) {

		JavaPairDStream<String, PriceData> stockPriceDStream = getPriceDStream(stockStream);
		JavaPairDStream<String, PriceData> windowDStream = stockPriceDStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA_VOLUME, DIFF_REDUCER_PRICE_DATA_VOLUME, Durations.minutes(windowSize),
				Durations.minutes(slidingInterval));
		
	
		
		return windowDStream;
	}

	private static JavaPairDStream<String, PriceData> getPriceDStream(JavaPairDStream<String, StockPrice> stockStream) {

		// restructure the RDD
		return stockStream.mapToPair(x -> {
			return new Tuple2<String, PriceData>(x._1, x._2.getPriceData());
		});
	}

}