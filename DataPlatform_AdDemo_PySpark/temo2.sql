


export OOZIE_URL=http://ip-172-31-45-59.ec2.internal:11000/oozie
oozie admin -sharelibupdate





su - hdfs



hadoop fs -put /var/lib/oozie/mysql-connector-java.jar hdfs://ip-172-31-45-59.ec2.internal:8020/user/oozie/share/lib/lib_20191114184457/sqoop/


hadoop fs -chown oozie hdfs://ip-172-31-45-59.ec2.internal:8020/user/oozie/share/lib/lib_20191114184457/sqoop/mysql-connector-java.jar


oozie admin -sharelibupdate


=========================


hadoop fs -mkdir -p /user/oozie/share/lib/lib_20191114184457/spark2


hdfs dfs -put /opt/cloudera/parcels/SPARK2/lib/spark2/jars/* /user/oozie/share/lib/lib_20191114184457/spark2/



hadoop fs -cp /user/oozie/share/lib/lib_20191114184457/spark/oozie-sharelib-spark* /user/oozie/share/lib/lib_20191114184457/spark2/


export OOZIE_URL=http://ip-172-31-45-59.ec2.internal:11000/oozie
 oozie admin –sharelibupdate spark2
oozie admin –shareliblist spark2

*/

hadoop fs -chown -R oozie:oozie /user/oozie/share/lib/lib_20191114184457/spark2/
hadoop fs -chmod -R 777 /user/oozie/share/lib/lib_20191114184457/spark2/


vi /etc/sqoop/conf/sqoop-site.xml

<property>
   <name>sqoop.metastore.client.autoconnect.url</name>
   <value>jdbc:hsqldb:hsql://ip-172-31-45-59.ec2.internal:16000/sqoop</value>
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


cp /etc/sqoop/conf/sqoop-site.xml /home/ec2-user/ad_demo/oozie_3/


 hadoop fs -mkdir -p /user/ec2-user/datawarehouse/



 flume-ng agent -n agent1 -c flume_conf -f ad_demo/flume/


 oozie job -oozie http://ip-172-31-45-59.ec2.internal:11000/oozie -config job.properties -run


 Container Virtual CPU Cores
yarn.nodemanager.resource.cpu-vcores



oozie job -kill 0000000-200105142556168-oozie-oozi-W


export OOZIE_URL=http://ip-172-31-45-59.ec2.internal:11000/oozie
oozie admin -sharelibupdate


oozie admin -shareliblist spark2

hadoop fs -put /opt/cloudera/parcels/SPARK2/lib/spark2/python/lib/*  /user/oozie/share/lib/lib_20191114184457/spark2/ 

*/
oozie admin -oozie http://ip-172-31-45-59.ec2.internal:11000/oozie/ -sharelibupdate



oozie admin -shareliblist spark2

hadoop fs -rm -r -skipTrash /user/ec2-user/raw/mysql/*
hadoop fs -rm -r -skipTrash /user/ec2-user/datawarehouse/*
hadoop fs -mkdir -p /user/ec2-user/datawarehouse/logs/

*/

sqoop job --delete products_import 
sqoop job --delete customers_import 
sqoop job --delete orders_import 
sqoop job --create customers_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table customers --incremental append --check-column customer_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/customers
sqoop job --create products_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table products --incremental append --check-column product_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/products
sqoop job --create orders_import -- import --connect jdbc:mysql://localhost:3306/Upgrad --username root --password 123 --table orders --incremental append --check-column order_id --last-value 0 --target-dir /user/ec2-user/raw/mysql/orders
sqoop job --list


#!/bin/bash

<cmd>

chmod +x hello.sh
./hello.sh


oozie job -oozie http://ip-172-31-45-59.ec2.internal:11000/oozie -config job.properties -run


oozie job -kill 0000000-200105155302886-oozie-oozi-W

 oozie job -oozie http://ip-172-31-45-59.ec2.internal:11000/oozie -config job.properties -run



> oozie sqoop
> MySQL jar
> Spark2 jars + python + permissions 
> web console
> yarn cores 


run flume or copy json

 clear HDFS
 start metastore
 create jobs
 run oozie