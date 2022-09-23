import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.WindowSpec


object window {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
 
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dataframe")
  sparkConf.set("spark.master", "local[2]")
  
  
   val spark= SparkSession.builder()
   .config(sparkConf)
   .getOrCreate()
      
   val invoiceDf=  spark.read
   .option("header", true)
   .option("InferSchema", true)
   .csv("file:///Users/91720/Downloads/windowdata.csv")
      
   val mywindow= Window.partitionBy("country").orderBy("Weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
    val df1 = invoiceDf.withColumn("RunningTotal", 
      sum("invoicevalue").over(mywindow))
      
      
 
}