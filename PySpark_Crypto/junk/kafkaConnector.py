
{"symbol":"BTC","timestamp":"2020-01-03 18:12:00","priceData":{"close":7324.44,"high":7324.53,"low":7324.41,"open":7324.49,"volume":-2149.1861}}
{"symbol":"ETH","timestamp":"2020-01-03 18:12:00","priceData":{"close":132.22,"high":132.22,"low":132.16,"open":132.16,"volume":-6298.49}}
{"symbol":"LTC","timestamp":"2020-01-03 18:12:00","priceData":{"close":41.77,"high":41.77,"low":41.76,"open":41.76,"volume":-2697.58}}
{"symbol":"XRP","timestamp":"2020-01-03 18:12:00","priceData":{"close":0.1925,"high":0.1925,"low":0.1925,"open":0.1925,"volume":2233.58}}



import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1


conf = SparkConf().setAppName("jupyter_Spark").setMaster("yarn-client")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)


ssc


brokers = "52.55.237.11:9092"
topic = "stockData"
kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

	
ssc.start()
ssc.awaitTermination()