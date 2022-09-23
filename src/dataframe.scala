import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dataframe extends App {
Logger.getLogger("org").setLevel(Level.ERROR)
  
 
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dataframe")
  sparkConf.set("spark.master", "local[2]")
  
  
   val spark= SparkSession.builder()
   .config(sparkConf)
   .getOrCreate()
      
   val orderDf=  spark.read
   .option("header", true)
   .option("InferSchema", true)
   .csv("file:///Users/91720/Downloads/order_data.csv")
   
   
     /* orderDf.select(
       count("*").as("row_count"),
       sum("quantity").as("totalQuantity"),
       avg("UnitPrice").as("AvgPrice"),
       countDistinct("InvoiceNo").as("countDistinct")).show()
 
       
      orderDf.selectExpr("count(*) as RowCount",
                         "sum(quantity) as totalquantity",
                         "avg(unitPrice) as avgprice",
                         "count(Distinct(InvoiceNo)) as countDistinct"
                         ).show()   
                
 
     orderDf.createOrReplaceTempView("sales")
     
     spark.sql("select count(*),sum(Quantity)	,avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()  
     
     val summarydf= orderDf.groupBy("Country","InvoiceNo")
     .agg(sum("Quantity").as("TotalQuantity"),
     sum(expr("Quantity * UnitPrice")).as("InvoiceVlaue") 
          ) 
      
      orderDf.createOrReplaceTempView("sales")
      
      orderDf.groupBy("country","InvoiceNo")
      .agg(expr("sum(quantity) as  TotalQuantit"),
          expr("sum(quantity * UnitPrice) as InvoiceValue")
          ).show()*/
                         
     orderDf.createOrReplaceTempView("sales")
     
        spark.sql("""select Country,InvoiceNo, sum(quantity) as TotalQuantity,sum(quantity * UnitPrice)as InvoiceNo
           from sales group By country, InvoiceNo""").show(true)
        
           
       //val mywindow = window.partitionBy("country")
           
   spark.stop()
   
}