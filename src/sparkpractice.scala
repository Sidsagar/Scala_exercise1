import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row._


object sparkpractice extends App{
    
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dataframe")
  sparkConf.set("spark.master", "local[2]")
  
  
   val spark= SparkSession.builder()
   .config(sparkConf)
   .getOrCreate()
   
   
   val mylist = List((1,"2021-07-04",1159,"CLOSED"),
       (2,"2021-07-24",256,"PENDING_PAYMENT"),
       (3,"2013-07-25",1599,"COMPLETE"),
       (4,"2019-07-04",1159,"CLOSED"))
       
          
    
     val orderDf =spark.createDataFrame(mylist).toDF("orderid","orderdate","customerid","status")
     
    val mynewDF = orderDf
    .withColumn("orderdate",unix_timestamp(col("orderdate")
    .cast(DateType)))
    .withColumn("newid", monotonically_increasing_id)
    .dropDuplicates("orderdate", "customerid")
    .sort("orderid")
    
    mynewDF.show()
   
   /*val input = sc.textFile("file:///Users/91720/Downloads/search_data-201008-180523.txt")
   
   val words = input.flatMap(x=>x.split(" "))
   
   val wordMap =words.map(x=>(x,1))
   
   val finalCount= wordMap.reduceByKey((a,b)=>a+b)
   
   finalCount.collect.foreach(println)
   
   scala.io.StdIn.readLine()*/
   
   
    }