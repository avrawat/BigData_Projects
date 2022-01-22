
def main():

	# Create the Spark Conext object
	sc = SparkContext("local[2]", "PySparkStreaming_Kafka_Crypto")
	
	# set the log level to WARN
	sc.setLogLevel("WARN")
	# Create the Spark Streaming Context - Set the batch interval to 1 minutes 
	ssc = StreamingContext(sc,60)
	ssc.checkpoint("checkpoint-dir")

	# Broadcast the stock Symbol names
	s1 = <Declair broadcast variable for 'BTC'>
	s2 = <Declair broadcast variable for 'ETH'>
	s3 = <Declair broadcast variable for 'LTC'>
	s4 = <Declair broadcast variable for 'XRP'>

	symbols = [s1,s2,s3,s4];

	# Define the window Size and Sliding interval
	windowSize = 10
	slidingInterval = 5

	# Kafka Parameters 
	
	brokers = "<Broker IP:Port Number>" # Public IP of the broker
	topic = "Topic name" # Topic name


	# Subscribe to the topic
	kvs = <Subscribe to Kafka Topic>
	# Filter out the JSON strings from the raw Kafka Stream 
	lines = kvs.map(lambda x: x[1])

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
		 < call the calculateAveragePrice(windowStream,symbol) method >


	# 2 Max Profit ##########################################################

	 < call the calculateMaxProfit(windowStream) method >
	
	# 3 Max stock volume ####################################################
	
	 < call the calculateMaxVolume(pairedStream,windowSize,slidingInterval) method > 

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




class MovingAverage():


	def calculateAveragePrice(self, stockStream, symbol):
		# Filter out the records for the Symbols
		symbolStream = stockStream.filter(<Apply the filter to get only the stock data for the symbol>)

		# Extract only the Close Price 
		stockPriceStream = symbolStream.map(lambda x:x[1].getPriceData().getClose())
		#stockPriceStream.pprint()
		# Now calculate the moving average taking results from all the RDDs 
		stockPriceStream.foreachRDD(lambda rdd:self.printAverage(symbol,rdd))

	def printAverage(self,symbol,priceRDD):
		# Reduce the RDD and calculate the average
		avg = <calculate the average using the data present in the priceRDD>
		# Print the results
		<print the results>


#------------------------------ MovingAverage -----------------------------------------

class MaxProfit():

	def calculateMaxProfit(self,stockStream):

		stockPriceStream = stockStream.map(lambda x:(x[0],<take the differene between close and open price>)) \
										.mapValues(lambda x:(x,1)) \
										.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
										.mapValues(lambda x: x[0]/x[1])
		

		# Extract the stock with maximum profit								
		stockPriceStream.foreachRDD(lambda rdd:self.printMaxProfit(rdd))

	def printMaxProfit(self,stockStream):
		# Sort the results in the Decreasing order and get the first element
		maxProfitStock = <sort the stockStream rdd w.r.t its key in Decreasing order and get only the first element>
		print("MAXIMUM PROFIT ANALYSIS:")
		<print the results>


#----------------------------- Max Profit -----------------------------



class MaxVolumeStock():

	def calculateMaxVolume(self,stocksPrice,windowSize,slidingInterval):

		# Get the DStream with Volume
		volumeStream = stocksPrice.map(lambda x:(x[0],<get the volume volume>))
		
		# Create the window DStream with Sum and Reducer methods
		windowStream = volumeStream.reduceByKeyAndWindow(lambda a,b:a+b,lambda a,b:a-b,windowSize*60,slidingInterval*60)

		# Get the maximum traded volume
		windowStream.foreachRDD(lambda rdd:self.printMaxVolume(rdd))


	def printMaxVolume(self,volumeStream):
		maxStockVolume = <Sort the RDD w.r.t the traded volume in decreasing order and get the first element>
		# Print the results
		print("VOLUME ANALYSIS:")
		<print the results>

#------------------------------- MaxVolumeStock ----------------------------------------