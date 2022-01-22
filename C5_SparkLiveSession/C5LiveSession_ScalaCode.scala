
/*
Steps to Run- 
1. Open Spark Shell on Amazon EC2
       > use command spark2-shell (if you have upgraded the default Spark version to 2.3.0)
       > use command spark2-shell (otherwise)

2. Download and ship the AirLineData.csv file to an HDFS Dir. 
3. Run the following commands one by one
4. Logic is same as the Java RDD programs 
5. Try Interpretting the syntax at each step with referece to the Java-RDD implementation. 

*/




val rawData = sc.textFile("/user/ec2-user/c5/input/AirLineData.csv")

//val rawData = sc.textFile("... HDFS path to the data file..")

val cleanData =  rawData.filter(x=>x.split(",")(0).startsWith("A"))

val rdd1 = cleanData.map(x=>{
val arr = x.split(",")
(arr(1).trim+":"+arr(18).trim,1)
})

val rdd2 = rdd1.reduceByKey((a,b)=>a+b)

val rdd3 = rdd2.map(x=>{
val arr = x._1.split(":")
(arr(0),(arr(1),x._2))
})

val rdd4 = rdd3.reduceByKey((a,b)=>{
if(a._2 > b._2) a 
else b
})

rdd4.sortBy(x=>x._2._2,false).collect.foreach(println)
