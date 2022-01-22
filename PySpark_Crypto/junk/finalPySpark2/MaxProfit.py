from PriceData import PriceData
from stocksPrice import stocksPrice


class MaxProfit:

	def __init__(self):
		pass

	def calculateMaxProfit(stockStream):

		stockPriceStream = stockStream.map(lambda x:(x[0],x[1].getPriceData().getClose()-x[1].getPriceData().getOpen())) \
										.mapValues(lambda x:(x,1)) \
										.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
										.mapValues(lambda x: x[0]/x[1]) \
										.map(lambda x:(x[1],x[0]))

		# Extract the stock with maximum profit								
		stockPriceStream.foreachRDD(lambda rdd:printMaxProfit(rdd))

#----------------------------- Max Profit -----------------------------

def printMaxProfit(stockStream):
	# Sort the results in the Decreasing order and get the first element
	maxProfitStock = stockStream.sortByKey(False).first()
	print("MAXIMUM PROFIT ANALYSIS:")
	print("\tStock "+ maxProfitStock[1] +" with maximum profit "+ str(maxProfitStock[0]))

#----------------------------- printMaxProfit -----------------------------