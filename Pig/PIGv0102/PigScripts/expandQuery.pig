REGISTER '/Users/ankit.sharma/C2Redev/PIGv0102/target/PIGv0102-0.0.1-SNAPSHOT.jar';
DEFINE CustomLoad udf.CustomLoad(',');
DEFINE expandQuery udf.ExpandQuery();
customer_queries  = LOAD '/Users/ankit.sharma/C2Redev/PIGv0102/DataPig/query_log.csv' USING CustomLoad() AS (userID:chararray,queryString:chararray,timeStamp:chararray);
real_queries = FILTER customer_queries BY userID neq 'bot';



expanded_queries = FOREACH real_queries GENERATE userID, expandQuery(queryString);
--DUMP expanded_queries


queries = FOREACH expanded_queries GENERATE userID, FLATTEN($1);
DUMP queries;