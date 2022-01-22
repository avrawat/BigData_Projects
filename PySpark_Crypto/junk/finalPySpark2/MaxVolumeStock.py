from PriceData import PriceData
from stocksPrice import stocksPrice


class MaxVolumeStock:

	def __init__(self):
		pass

	def calculateMaxVolume(stocksPrice,windowSize,slidingInterval):

		# Get the DStream with Volume
		volumeStream = stocksPrice.map(lambda x:(x[0],x[1].getPriceData().getVolume()))
		
		# Create the window DStream with Sum and Reducer methods
		windowStream = volumeStream.reduceByKeyAndWindow(lambda a,b:a+b,lambda a,b:a-b,windowSize*60,slidingInterval*60)

		# Get the maximum traded volume
		windowStream.foreachRDD(lambda rdd:printMaxVolume(rdd))

#----------------------------------- calculateMaxVolume -------------------------------------------
def printMaxVolume(volumeStream):

	# Sort the RDD w.r.t the traded volume in decreasing order and get the first element
	maxStockVolume = volumeStream.map(lambda x:(x[1],x[0])) \
								.sortByKey(False).first()
	# Print the results
	print("VOLUME ANALYSIS:")
	print("\tStock "+maxStockVolume[1]+" with maximum volume "+str(maxStockVolume[0]))

#------------------------------- printMaxVolume ---------------------------------------------------