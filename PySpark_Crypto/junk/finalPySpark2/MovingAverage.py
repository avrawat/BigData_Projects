from PriceData import PriceData
from stocksPrice import stocksPrice


class MovingAverage:

	def __init__(self):
		pass

	def calCulateAveragePrice(stockStream, symbol):

		# Filter out the records for the Symbols
		symbolStream = stockStream.filter(lambda x:x[0]==symbol.value)

		# Extract only the Close Price 
		stockPriceStream = symbolStream.map(lambda x:x[1].getPriceData().getClose())

		#stockPriceStream.pprint()

		# Now calculate the moving average taking results from all the RDDs 
		stockPriceStream.foreachRDD(lambda rdd:printAverage(symbol,rdd))

#--------------------------- Moving Average --------------------------------------

def printAverage(symbol,priceRDD):
	print("MOVING AVERAGE ANALYSIS: "+symbol.value)
	print("\tRDD count(Number of stock prices): " + str(priceRDD.count()))

	# Reduce the RDD and calculate the average
	avg = priceRDD.reduce(lambda a,b: a+b) / priceRDD.count()

	# Print the results
	print("\tAverage Price: "+str(avg))

#--------------------------- printAverage ---------------------------------------


