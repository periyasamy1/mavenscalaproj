package demo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object EmployeeDetails {
 def main(args:Array[String]):Unit = {

   val spark = SparkSession.builder.master("local").appName("Employee Assignment").getOrCreate()
   spark.sparkContext.setLogLevel("WARN")
   val df = spark.read.option("header","true").csv("src\\main\\resources\\emp_data.txt")
   df.show()

   //val df1 = spark.read.option("header","true").csv("src\\main\\resources\\empdetails.csv")
   //df1.show()

   val w3 = Window.partitionBy("designation").orderBy(col("salary").desc)
            df.withColumn("row",row_number.over(w3))
            .show()

   // write data into file in csv format
   //df.write.option("header","true").csv("src\\main\\resources\\empData")

   // Find first record of the file
   val emp_header = df.first()
   println(emp_header)
 }
}