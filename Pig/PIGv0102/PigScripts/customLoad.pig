REGISTER '/Users/ankit.sharma/C2Redev/PIGv0102/target/PIGv0102-0.0.1-SNAPSHOT.jar';
DEFINE CustomLoad udf.CustomLoad(',');


customer_queries  = LOAD '/Users/ankit.sharma/C2Redev/PIGv0102/DataPig/query_log.csv' USING CustomLoad() AS (userID:chararray,queryString:chararray,timeStamp:chararray);
--customer_queries  = LOAD '/home/abhinavrawat/eclipse-workspace2/PIGv0102/DataPig/query_log.csv' USING PigStorage(',') AS (userID:chararray,queryString:chararray,timeStamp:chararray);
DUMP customer_queries;

real_queries = FILTER customer_queries BY userID neq 'bot';
DUMP real_queries;