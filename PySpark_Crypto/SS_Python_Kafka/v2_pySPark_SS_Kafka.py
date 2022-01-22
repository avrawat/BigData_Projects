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
     .groupBy(window("timestamp", "3 minutes", "2 minutes"),"symbol") \
     .agg(avg("close").alias("sma"))

query2 = sma \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()


query1.awaitTermination()
query2.awaitTermination()



def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.