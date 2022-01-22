MovieLensData = LOAD '/user/ec2-user/spark_assignment/input/yellow_tripdata*'
                        using PigStorage(',') as
                                                (VendorID,
                                                tpep_pickup_datetime,
                                                tpep_dropoff_datetime,
                                                passenger_count,
                                                trip_distance,
                                                RatecodeID,
                                                store_and_fwd_flag,
                                                PULocationID,
                                                DOLocationID,
                                                payment_type,
                                                fare_amount,
                                                extra,
                                                mta_tax,
                                                tip_amount,
                                                tolls_amount,
                                                improvement_surcharge,
                                                total_amount
                                                );


MovieLensData_Clean = FILTER MovieLensData BY NOT (VendorID == 'VendorID') OR (VendorID =='') OR (VendorID eq null);

ReqData = FOREACH MovieLensData_Clean GENERATE payment_type AS payment_type, 1L AS CNT;

ReqDataGrp = GROUP ReqData BY payment_type;

FinalData = FOREACH ReqDataGrp GENERATE FLATTEN(group), SUM(ReqData.CNT) AS CNT;

FinalDataSort = ORDER FinalData BY CNT;

STORE FinalDataSort INTO '/user/ec2-user/spark_assignment/output/pig/q3' USING PigStorage(',');

