REGISTER '/Users/ankit.sharma/C2Redev/PIGv3/target/PIGv3-0.0.1-SNAPSHOT.jar';
DEFINE distributerRevenue udf.DistributeRevenue;
results = LOAD '/Users/ankit.sharma/C2Redev/PIGv3/DataPig/results.csv' USING PigStorage(',') AS (queryString:chararray,url:chararray,rank:int);
revenue = LOAD '/Users/ankit.sharma/C2Redev/PIGv3/DataPig/revenue.csv' USING PigStorage(',') AS (queryString:chararray,adSlot:chararray,amount:float);
grouped_data = COGROUP results BY queryString, revenue BY queryString;
DUMP grouped_data;

url_revenue = FOREACH grouped_data GENERATE FLATTEN (distributerRevenue(results,revenue));
DUMP url_revenue;