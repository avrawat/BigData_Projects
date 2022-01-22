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


ReqData = FILTER MovieLensData_Clean BY RatecodeID == '4';


STORE ReqData INTO '/user/ec2-user/spark_assignment/output/pig/q2/' USING PigStorage(',');