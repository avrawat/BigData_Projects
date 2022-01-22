from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("SS_Kafka") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

priceSchema = StructType([ \
						StructField("close", DoubleType(), True), \
						StructField("high", DoubleType(), True), \
						StructField("low", DoubleType(), True), \
						StructField("open", DoubleType(), True), \
						StructField("volume", DoubleType(), True) \
					])


jsonSchema = StructType([ \
							StructField("symbol", StringType(), True), \
							StructField("timestamp", TimestampType(), True), \
							StructField("priceData",priceSchema)
			 	])
				

	# brokers = "52.55.237.11:9092" # Public IP of the broker
	# topic = "stockData" # Topic name
	# groupID = "Rawat_" + str(datetime.now()) # group ID
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "52.55.237.11:9092") \
    .option("subscribe", "stockData") \
    .load()



parsed = lines \
		.selectExpr("CAST(value AS STRING) as json") \
		.select(from_json("json",schema=jsonSchema).alias("data")) \
		.select("data.*") \
		.select("symbol", "timestamp","priceData.*")



 # Start running the query that prints the running counts to the console
query1 = parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


sma = parsed \
     .groupBy(window("timestamp", "10 minutes", "5 minutes"),"symbol") \
     .agg(avg("close").alias("sma"))

query2 = sma \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Remove the av_close and av_open col
profit = parsed \
	.groupBy(window("timestamp","10 minutes","5 minutes"),"symbol") \
	.agg(avg("close").alias("av_close"),avg("open").alias("av_open")) \
	.withColumn('profit', col('av_close')-col('av_open')) \
	.orderBy(col('profit').desc()) \
	.limit(1)

	
query3 = profit \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


maxVol = parsed \
	.groupBy(window("timestamp","10 minutes","5 minutes"),"symbol") \
 	.agg(sum(abs(col("volume"))).alias("total_volume")) \
 	.orderBy(col('total_volume').desc()) \
	.limit(1)


query4 = maxVol \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()