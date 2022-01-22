package solution;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


import scala.Tuple2;
import schemaClasses.*;

public class SparkApplication {
	
	
	//KafKa parameters
	private static String brokerID;
	private static String groupID;
	private static String topicName;
	
	// currency codes
	private static String[] currencies = {"BTC", "ETH", "LTC", "XRP"};
	public static Profit profit[] = new Profit[4];
	private static String outputPath = "outputData";
	
	private static int windowSize;
	private static int slidingInterval;

	public static void main(String[] args) throws Exception {
	//	PrintStream redir = new PrintStream(new File("output.txt"));
	//	System.setOut(redir);
		// spark configuration and Streaming context	
		SparkConf conf = new SparkConf().setAppName("Kafka-SparkStreaming").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
		jssc.sparkContext().setLogLevel("WARN");
		
		jssc.checkpoint("checkpoint-dir");
		 
		/*
		SparkApplication.brokerID = args[0];
		SparkApplication.topicName = args[1];
		SparkApplication.groupID = args[2];
		
		*/
		
		SparkApplication.brokerID = "34.206.133.62";
		SparkApplication.topicName = "stockData";
		SparkApplication.groupID = "rawatAB02763CWIDFGTH";
		
		
		
		// Kafka Consumer parameters
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", brokerID + ":9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", groupID);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		// topic list
		Collection<String> topics = Arrays.asList(topicName);

		// get the input D Stream
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		// Remove the key
		JavaDStream<String> val = stream.map(x -> x.value());
		
		
		//Parsing the JSON string to Java objects
		JavaPairDStream<String, StockPrice> stockStream = SparkApplication.convertIntoPairDStream(val);
		
		//print the input stream
		stockStream.cache().print();
		
		String[] stockArray = currencies;
		
		//1. ============================================================================================
				
		// Calculate the moving average of each stock
		// in minute
		windowSize = 3;
		slidingInterval = 2;
		
		for (String symbol : stockArray) {
			MovingAverage.calculateAveragePrice(stockStream, symbol, outputPath,windowSize,slidingInterval);
		}
		
		//2. ===========================================================================================
		// in minute
		windowSize = 3;
		slidingInterval = 2;
		
		// calculate maximum profit
		MaxProfit.calculateProfit(stockStream, outputPath,windowSize,slidingInterval);
		
		//3. ==========================================================================================
		// in minute
		windowSize = 3;
		slidingInterval = 2;

		// Calculate volume traded for each stock
		VolumeStocks.calculateVolume(stockStream, outputPath,windowSize,slidingInterval);
		//=============================================================================================
		
		jssc.start();
		jssc.awaitTermination();

	}

	private static JavaPairDStream<String, StockPrice> convertIntoPairDStream(JavaDStream<String> json) {

		return json.mapToPair(x -> {
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<StockPrice> mapType = new TypeReference<StockPrice>() {};
			StockPrice stockList = mapper.readValue(x, mapType);
			return new Tuple2<String, StockPrice>(stockList.getSymbol(), stockList);
		});

	}

}
