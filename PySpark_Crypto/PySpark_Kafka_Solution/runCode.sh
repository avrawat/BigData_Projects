#!/bin/bash
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pySpark_Kafka_Project.py --py-files PriceData.py, stocksPrice.py, MovingAverage.py, MaxProfit.py, MaxVolumeStock.py
