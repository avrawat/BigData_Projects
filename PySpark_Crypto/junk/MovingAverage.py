from PriceData import PriceData
from stocksPrice import stocksPrice


class MovingAverage:

	def __init__(self):
		pass

	def calCulateAveragePrice(stockStream, symbol):

		symbolStream = stockStream.filter(lambda x:x[0]==symbol.value)
		stockPriceStream = symbolStream.map(lambda x:x[1].getPriceData().getClose())

		#stockPriceStream.pprint()
		#windowStream = stockPriceStream.window(window*60,sliding*60)
		stockPriceStream.foreachRDD(lambda rdd:printAverage(symbol,rdd))


def printAverage(symbol,priceRDD):
	print("MOVING AVERAGE ANALYSIS: "+symbol.value)
	print("\tRDD count(Number of stock prices): " + str(priceRDD.count()))
	avg = priceRDD.reduce(lambda a,b: a+b) / priceRDD.count()
	print("\tAverage Price: "+str(avg))



