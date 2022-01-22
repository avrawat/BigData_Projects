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

	sc = SparkContext("local[2]", "PySparkStreaming_Kafka_Crypto")
	sc.setLogLevel("WARN")
	ssc = StreamingContext(sc,60)
	ssc.checkpoint("checkpoint-dir")

	s1 = sc.broadcast("BTC")
	s2 = sc.broadcast("ETH")
	s3 = sc.broadcast("LTC")
	s4 = sc.broadcast("XRP")

	symbols = [s1,s2,s3,s4];
	windowSize = 3
	slidingInterval = 2

	brokers = "52.55.237.11:9092"
	topic = "stockData"
	groupID = "Rawat_" + str(datetime.now())


	kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
	lines = kvs.map(lambda x: x[1])

	lines.pprint()



	rdd1 = lines.map(lambda rawStream:getStockPriceObject(rawStream))
	rdd1.pprint()
	rdd2 = rdd1.map(lambda x:(x.getSymbol(),x))
	#rdd2.pprint()

	windowStram = rdd2.window(windowSize*60,slidingInterval*60)
	for symbol in symbols:
		MovingAverage.calCulateAveragePrice(windowStram,symbol)

	
	MaxProfit.calculateMaxProfit(windowStram)

	MaxVolumeStock.calculateMaxVolume(rdd2,windowSize,slidingInterval)

	ssc.start()
	ssc.awaitTermination()


def getStockPriceObject(jsonString):
	jsonOb = json.loads(jsonString)
	priceDataOb = PriceData(jsonOb['priceData']['open'],jsonOb['priceData']['high'],jsonOb['priceData']['low'],jsonOb['priceData']['close'],jsonOb['priceData']['volume'])
	stockDataOb = stocksPrice(jsonOb['symbol'],jsonOb['timestamp'],priceDataOb)
	return stockDataOb

if __name__== "__main__":
	main()


# spark-submit --jars spark-streaming-kafka-0-8_2.11-2.4.4.jar pySpark.py,schemaClasses.py 
# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark.py --py-files PriceData.py, stocksPrice.py 
# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark.py --py-files newclasses.zip
