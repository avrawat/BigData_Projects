MovieLensData = LOAD '/user/ec2-user/spark_assignment/input/yellow*'
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


ReqData = FILTER MovieLensData_Clean BY VendorID == '2' AND
                                        tpep_pickup_datetime == '2017-10-01 00:15:30' AND
                                        tpep_dropoff_datetime == '2017-10-01 00:25:11' AND
                                        passenger_count == '1' AND trip_distance == '2.17';


STORE ReqData INTO '/user/ec2-user/spark_assignment/output/pig/q1' USING PigStorage(',');