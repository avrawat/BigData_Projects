

wget https://cdn.upgrad.com/UpGrad/temp/52d38d9f-7518-4257-be66-c20028aa2a85/Course3_DataSets_customers.csv
wget https://cdn.upgrad.com/UpGrad/temp/27b30793-5edd-41d6-9017-104b5d38a2d9/Course3_DataSets_orders.csv
wget https://cdn.upgrad.com/UpGrad/temp/47f85878-a2a8-4bc5-8804-63a8d753f122/Course3_DataSets_products.csv



create database Upgrad;

use Upgrad;

CREATE Table customers (
customer_id INT,
customer_name VARCHAR(100), 
customer_gender VARCHAR(1), 
created_at DATETIME, 
PRIMARY KEY (customer_id));


LOAD DATA LOCAL INFILE '/home/ec2-user/ad_demo/Course3_DataSets_customers.csv' INTO TABLE customers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


CREATE Table products (
product_id INT, 
product_name VARCHAR(100), 
product_season VARCHAR(10), 
product_gender VARCHAR(1), 
created_at DATETIME,
PRIMARY KEY (product_id));



LOAD DATA LOCAL INFILE '/home/ec2-user/ad_demo/Course3_DataSets_products.csv' INTO TABLE products FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;



CREATE Table orders (
order_id INT,
product_id INT,
quantity INT,
created_at DATETIME,
PRIMARY KEY (order_id));

LOAD DATA LOCAL INFILE '/home/ec2-user/ad_demo/Course3_DataSets_orders.csv' INTO TABLE orders FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;






hadoop fs -mkdir  -p /user/ec2-user/raw/mysql/customers
hadoop fs -mkdir  -p /user/ec2-user/raw/mysql/products
hadoop fs -mkdir  -p /user/ec2-user/raw/mysql/orders


sqoop job --create customers_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table customers --incremental append --check-column customer_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/customers
sqoop job --create products_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table products --incremental append --check-column product_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/products
sqoop job --create orders_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table orders --incremental append --check-column order_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/orders



sqoop job -exec customers_import

sqoop job -exec products_import

sqoop job -exec orders_import

==================================================

Flume


hadoop fs -mkdir -p /user/ec2-user/raw/logs/ 








#Flume Configuration starts
  
#Define a file channel called fileChannel on agent1
agent1.channels.fileChannel1.type=file
agent1.channels.fileChannel1.capacity=200000
agent1.channels.fileChannel1.transactionCapacity=1000

#Define a source for agent1
agent1.sources.source1.type=spooldir
agent1.sources.source1.spoolDir=/home/ec2-user/ad_demo/flume/flumeSpool
agent1.sources.source1.fileHeader=false
agent1.sources.source1.fileSuffix=.COMPLETED

#Define sink for agent1
agent1.sinks.hdfs-sink1.type=hdfs	
agent1.sinks.hdfs-sink1.hdfs.path=hdfs://ip-172-31-45-59.ec2.internal:8020/user/ec2-user/raw/logs/
agent1.sinks.hdfs-sink1.hdfs.batchSize=1000
agent1.sinks.hdfs-sink1.hdfs.rollSize=5000
agent1.sinks.hdfs-sink1.hdfs.rollInterval=500
agent1.sinks.hdfs-sink1.hdfs.rollCount=0
agent1.sinks.hdfs-sink1.hdfs.writeFormat=Text
agent1.sinks.hdfs-sink1.hdfs.filePrefix = logdata-%d-%H-%m
agent1.sinks.hdfs-sink1.hdfs.useLocalTimeStamp = true

agent1.sinks.hdfs-sink1.hdfs.fileType = DataStream
agent1.sources.source1.channels = fileChannel1
agent1.sinks.hdfs-sink1.channel = fileChannel1

agent1.sinks =  hdfs-sink1
agent1.sources = source1
agent1.channels = fileChannel1



===================================

Oozie

export OOZIE_URL=http://ip-10-0-0-90.ec2.internal:11000/oozie

oozie admin -sharelibupdate

/var/lib/oozie/mysql-connector-java.jar


su - hdfs


hadoop fs -put /var/lib/oozie/mysql-connector-java.jar hdfs://ip-10-0-0-90.ec2.internal:8020/user/oozie/share/lib/lib_20190617105724


hadoop fs -chown oozie hdfs://ip-10-0-0-90.ec2.internal:8020/user/oozie/share/lib/lib_20190617105724/mysql-connector-java.jar


hadoop fs -ls hdfs://ip-10-0-0-90.ec2.internal:8020/user/oozie/share/lib/lib_20190617105724

oozie admin -sharelibupdate


logout



--------------

Spark Oozie

su - hdfs

hadoop fs -mkdir -p /user/oozie/share/lib/lib_20190617105724/spark2



hdfs dfs -put /opt/cloudera/parcels/SPARK2/lib/spark2/jars/* /user/oozie/share/lib/lib_20190617105724/spark2



hadoop fs -cp /user/oozie/share/lib/lib_20190617105724/spark/oozie-sharelib-spark* /user/oozie/share/lib/lib_20190617105724/spark2/


export OOZIE_URL=http://ip-10-0-0-90.ec2.internal:11000/oozie




oozie admin –sharelibupdate spark2


oozie admin -shareliblist spark2


*/ 
====================

<property>
   <name>sqoop.metastore.client.autoconnect.url</name>
   <value>jdbc:hsqldb:hsql://ip-10-0-0-90.ec2.internal:16000/sqoop</value>
   <description>The connect string to use when connecting to a
     job-management metastore. If unspecified, uses ~/.sqoop/.
     You can specify a different path here.
   </description>
 </property>

<property>
   <name>sqoop.metastore.client.record.password</name>
   <value>true</value>
   <description>If true, allow saved passwords in the metastore.
   </description>
 </property>




sudo -u sqoop sqoop-metastore

sqoop job --create customers_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table customers --incremental append --check-column customer_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/customers
sqoop job --create products_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table products --incremental append --check-column product_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/products
sqoop job --create orders_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table orders --incremental append --check-column order_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/orders


 hadoop fs -mkdir -p /user/ec2-user/datawarehouse/


oozie job -oozie http://ip-10-0-0-90.ec2.internal:11000/oozie -config job_c3_demo.properties -run


sqoop job -exec products_import --meta-connect jdbc:hsqldb:hsql://ip-10-0-0-90.ec2.internal:16000/sqoop
sqoop job -exec customers_import --meta-connect jdbc:hsqldb:hsql://ip-10-0-0-90.ec2.internal:16000/sqoop
sqoop job -exec orders_import --meta-connect jdbc:hsqldb:hsql://ip-10-0-0-90.ec2.internal:16000/sqoop


​sqoop job --delete products_import 
​sqoop job --delete customers_import 
​sqoop job --delete orders_import 




sqoop import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table customers --target-dir /user/ec2-user/raw/mysql/customers


hdfs dfs -put /opt/cloudera/parcels/SPARK2/lib/spark2/python/lib/* /user/oozie/share/lib/lib_20190617105724/spark2

*/
export OOZIE_URL=http://ip-10-0-0-90.ec2.internal:11000/oozie
oozie admin -sharelibupdate



# https://thatbigdata.blogspot.com/2018/05/how-to-submit-python-spark2-action-via.html

oozie admin -shareliblist spark2
hadoop fs -put /opt/cloudera/parcels/SPARK2/lib/spark2/python/lib/* /user/oozie/share/lib/lib_20190617105724/spark2/


