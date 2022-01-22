package solution;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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

public class SparkApplication2 {
		
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
		
		SparkApplication2.brokerID = "52.55.237.11";
		SparkApplication2.topicName = "stockData";
		SparkApplication2.groupID = "rawatA-" + System.currentTimeMillis()/1000;
		
		
		
		// Kafka Consumer parameters
		 // Split the topics if multiple values are passed.
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topicName));
        
        // Define a new HashMap for holding the kafka information. 
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerID+":9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct Kafka stream with brokers and topics/
        // LocationStrategy with prefer consistent allows partitions to be distributed consistently to the spark executors.
        // CosumerStrategy allows to subscribe to the Kafka topics.
        // JavaInputDStream is a continuous input stream associated to the source.
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
		
		
		
		// Remove the key
		JavaDStream<String> val = stream.map(x -> x.value());
		
		
		//Parsing the JSON string to Java objects
		JavaPairDStream<String, StockPrice> stockStream = SparkApplication2.convertIntoPairDStream(val);
		
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
