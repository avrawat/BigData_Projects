package solution;

import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import schemaClasses.*;
import scala.Tuple2;

public class MovingAverage implements Serializable {
	
	// in minute 
	private static int windowSize;
	private static int slidingInterval;


	private static final long serialVersionUID = 1L;

	private static Function2<PriceData, PriceData, PriceData> SUM_REDUCER_PRICE_DATA_CLOSE = (a, b) -> {
		a.setClose(a.getClose() + b.getClose());
		return a;
	};

	public static void calculateAveragePrice(JavaPairDStream<String, StockPrice> stockStream, String symbol,
			String outputpath, int window, int sliding) {

		
		MovingAverage.windowSize = window;
		MovingAverage.slidingInterval = sliding;
		
		JavaPairDStream<String, PriceData> stockPriceStream = getPriceDStream(stockStream,symbol);

		JavaPairDStream<String, PriceData> windowDStream = stockPriceStream.window(Durations.minutes(windowSize),
				Durations.minutes(slidingInterval));
		
	
		//windowDStream.print();
		
		windowDStream.foreachRDD(rdd -> {
			JavaRDD<PriceData> stockPriceRdd = rdd.map(f -> f._2).cache();

			// Print the count of the RDDS present in the window
			System.out.println("RDD count(Number of stock prices): " + stockPriceRdd.count());

			// Get the average price of the stock
			double avg = stockPriceRdd.reduce(SUM_REDUCER_PRICE_DATA_CLOSE).getClose() / stockPriceRdd.count();

			System.out.println(String.format("MOVING AVERAGE ANALYSIS: %s Average Price: %s", symbol, avg));
			System.out.println();
		});
	}
	
	private static JavaPairDStream<String, PriceData> getPriceDStream(JavaPairDStream<String,StockPrice> stockStream, String symbol){
		// filter the data related to one Stock only
		JavaPairDStream<String,StockPrice> symbolStream = stockStream.filter(x->x._1.equals(symbol));
		// get only the PriceData object
		return symbolStream.mapToPair(x->{		
			return new Tuple2<String,PriceData>(x._1,x._2.getPriceData());
		});
	}
}




















