from PriceData import PriceData
from stocksPrice import stocksPrice


class VolumeStock:

	def __init__(self):
		pass

	def calculateMaxVolume(stocksPrice,windowSize,slidingInterval):

		volumeStream = stocksPrice.map(lambda x:(x[0],x[1].getPriceData().getVolume()))
		windowStream = volumeStream.reduceByKeyAndWindow(lambda a,b:a+b,lambda a,b:a-b,windowSize*60,slidingInterval*60)

def printMaxVolume(volumeStream):
	maxStockVolume = volumeStream.map(lambda x:(x[1],x[0])) \
								.sortByKey(False).first()
	print("VOLUME ANALYSIS:")
	print("\tStock "+maxStockVolume[0]+" with maximum volume "+str(maxStockVolume[1]))
