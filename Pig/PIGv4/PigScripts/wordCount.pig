
REGISTER '/Users/ankit.sharma/C2Redev/PIGv4/target/PIGv4-0.0.1-SNAPSHOT.jar';
DEFINE mymap udf.MyMapper();
DEFINE myreduce udf.MyReducer();
input_file  = LOAD '/Users/ankit.sharma/C2Redev/PIGv4/DataPig/dropbox-policy.txt' USING PigStorage() AS (line:chararray);

-- map
map_result = FOREACH input_file GENERATE FLATTEN(mymap(*));
DUMP map_result; 

-- combine
url_groups = GROUP map_result BY $1;
DUMP url_groups;

-- reduce
url_output = FOREACH url_groups GENERATE myreduce(*);
DUMP url_output;