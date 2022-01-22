from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
from datetime import datetime
import json
from PriceData import PriceData
from stocksPrice import stocksPrice
from MovingAverage import MovingAverage
from MaxProfit import MaxProfit
from MaxVolumeStock import MaxVolumeStock


def main():

	# Create the Spark Conext object
	sc = SparkContext("local[2]", "PySparkStreaming_Kafka_Crypto")
	
	# set the log level to WARN
	sc.setLogLevel("WARN")
	# Create the Spark Streaming Context - Set the batch interval to 1 minutes 
	ssc = StreamingContext(sc,60)
	ssc.checkpoint("checkpoint-dir")

	# Broadcast the stock Symbol names
	s1 = sc.broadcast("BTC")
	s2 = sc.broadcast("ETH")
	s3 = sc.broadcast("LTC")
	s4 = sc.broadcast("XRP")

	symbols = [s1,s2,s3,s4];

	# Define the window Size and Sliding interval
	windowSize = 3
	slidingInterval = 2

	# Kafka Parameters 
	
	brokers = "52.55.237.11:9092" # Public IP of the broker
	topic = "stockData" # Topic name
	groupID = "Rawat_" + str(datetime.now()) # group ID

	# Subscribe to the topic
	kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
	# Filter out the JSON strings from the raw Kafka Stream 
	lines = kvs.map(lambda x: x[1])

	# Print the input JSON string
	# lines.pprint()


	# Parse the JSON string
	parsedStream = lines.map(lambda rawStream:getStockPriceObject(rawStream))
	parsedStream.pprint()

	# Create a Pair DStream, Key - Stock Symbol, Value - Parsed JSON object
	pairedStream = parsedStream.map(lambda x:(x.getSymbol(),x))
	#rdd2.pprint()

	# get the windowed DStream
	windowStream = pairedStream.window(windowSize*60,slidingInterval*60)

	# 1 Moving Average ######################################################
	for symbol in symbols:
		MovingAverage.calCulateAveragePrice(windowStream,symbol)

	# 2 Max Profit ##########################################################
	
	MaxProfit.calculateMaxProfit(windowStream)
	
	# 3 Max stock volume ####################################################
	MaxVolumeStock.calculateMaxVolume(pairedStream,windowSize,slidingInterval)

	#########################################################################
	
	ssc.start()
	ssc.awaitTermination()
#--------------------------------------- main --------------------------------

def getStockPriceObject(jsonString):
	# Parse the JSON object to a Dict
	jsonOb = json.loads(jsonString)

	# Create the Price Data Object
	priceDataOb = PriceData(jsonOb['priceData']['open'],jsonOb['priceData']['high'],jsonOb['priceData']['low'],jsonOb['priceData']['close'],jsonOb['priceData']['volume'])
	
	# Create the Stocl Data Object
	stockDataOb = stocksPrice(jsonOb['symbol'],jsonOb['timestamp'],priceDataOb)
	return stockDataOb

#--------------------------------------- getStockPriceObject -------------------


if __name__== "__main__":
	main()

#-------------------------------------------------------------------------------


# spark-submit --jars spark-streaming-kafka-0-8_2.11-2.4.4.jar pySpark.py,schemaClasses.py 
# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark.py --py-files PriceData.py, stocksPrice.py 
# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark.py --py-files newclasses.zip
