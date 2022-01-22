
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame



def main():
	
	# Creating Spark Session Object
	spark = SparkSession \
	    .builder \
	    .appName("Clean_Logs_with_PySpark__oozie") \
	    .master("yarn") \
	    .getOrCreate()

	# Creating SparkContext
	sc = spark.sparkContext
	# setting log levels to WARNING
	sc.setLogLevel("WARN")

	# Path Variables
	# Hadoop NameNode Port- Private IP of the instance 
	HDFSLINK = "hdfs://ip-172-31-45-59.ec2.internal:8020/"
	# Flume HDFS sink directory - contains logs in raw format
	logsPath = "/user/ec2-user/raw/logs/"
	# Hive Warehouse directory - contains cleaned logs
	wareHouseDir = "/user/ec2-user/datawarehouse/logs/"

	# https://diogoalexandrefranco.github.io/interacting-with-hdfs-from-pyspark/
	# Accessing HDFS using Py4J gateway
	# The org.apache.hadoop.fs.FileSystem is a Java Class to acess HDFS
	URI = sc._gateway.jvm.java.net.URI
	Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
	FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
	fs = FileSystem.get(URI(HDFSLINK), sc._jsc.hadoopConfiguration())

	# Create a Path object for the raw log directory
	rawLogsDir = Path(logsPath)

	# Fetch the file names to be cleaned
	filesToProcess = fetchLogFilesToProcess(fs, rawLogsDir)

	# If we have at least one file to clean
	if len(filesToProcess) > 0:
		# Clean the logs
		cleanedLogDF = cleanLogs(spark,filesToProcess)

		# Save the cleaned log DF to the ware house directory
		curDateTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
		cleanedLogDF.write.csv(HDFSLINK + wareHouseDir + curDateTime)
		print("Cleaned logs have been written to - "+ wareHouseDir+curDateTime)
		# Append the processed log file with ".COMPLETED" as suffix
		appendFilesWithCompleted(fs,filesToProcess,Path)

	print("Total " + str(len(filesToProcess)) +" files Processed")

########################################################################

def fetchLogFilesToProcess(fs, directoryPath):

	# Fetch the raw log file names to be cleaned
	FILE_STATUSES = [];
	try:
		# for each file in the directory check if it doesn't have .COMPLETED as suffix 
		for filePath in fs.listStatus(directoryPath):
			stringPath = filePath.getPath().toString().encode('ascii','ignore')
			if filePath.isFile() and not(stringPath.endswith(".COMPLETED")):
				# Create a list of file name to be cleaned
				FILE_STATUSES.append(filePath)
	except:
		raise Exception("Exception while getting the file status")

	return FILE_STATUSES

########################################################################

def cleanLogs(spark,filesToProcess):
	# Clean the raw log files
	DFList=[]
	# for each raw file
	try:
		for filePath in filesToProcess:
			stringPath = filePath.getPath().toString().encode('ascii','ignore')
			# Load the file into a Spark DF
			rawDF = spark.read.json(stringPath)
			# Select the relevent columns
			newDF = rawDF.select("customer_id", "product_id", "request_id", "created_at")
			# Create the list of cleaned DFs
			DFList.append(newDF)

		# Append all the cleaned DFs into one
		final_DF = reduce(DataFrame.unionAll,DFList)
	except:
		raise Exception("Exception while cleaning the logs")
    
	return final_DF

########################################################################

def appendFilesWithCompleted(fs,filesToProcess,Path):
	try:
		# For each file which was processed 
		for filePath in filesToProcess:
			fileName = filePath.getPath().toString().encode('ascii','ignore')
			# Remove the HDFS URL and append ".COMPLETED" as suffix
			newFileName = fileName.split("8020")[1] + ".COMPLETED"
			# Rename the file
			fs.rename(filePath.getPath(), Path(newFileName))
			#print("Completed Processing - "+ newFileName)
	except:
		raise Exception("Exception while renaming the logs")

########################################################################

if __name__== "__main__":
	main()

########################################################################