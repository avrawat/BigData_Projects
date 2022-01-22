from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
from datetime import datetime



sc = SparkContext("local[2]", "PySparkStreaming_Kafka_Crypto")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,60)

brokers = "52.55.237.11:9092"
topic = "stockData"
groupID = "Rawat_" + str(datetime.now())
kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])

lines.pprint()


ssc.start()
ssc.awaitTermination()




# spark-submit --jars spark-streaming-kafka-0-8_2.11-2.4.4.jar pySpark.py 
