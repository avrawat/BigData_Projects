Local Mode




> spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark_Kafka_Project.py --py-files PriceData.py, stocksPrice.py, MovingAverage.py, MaxProfit.py, MaxVolumeStock.py

# To create the zip follow these steps - # https://stackoverflow.com/questions/36461054/i-cant-seem-to-get-py-files-on-spark-to-work

> spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark_Kafka_Project.py --py-files dependencies.zip




EC2 

> sudo yum install zip

> from the dependencies folder itself run the following command. 

>  zip -r ../dependencies.zip .


spark2-submit --py-files dependencies.zip pySpark_Kafka_Project.py