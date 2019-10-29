package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Car {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("testAPP").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val file = spark.read.format("csv").option("header","true").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("inferSchema","true").load("s3://s3-test-550-2019/craigslistcarsFull.csv")
    val sqlC = new org.apache.spark.sql.SQLContext(sc)
    val temp_v = file.createOrReplaceTempView("File")
    val tab_1 = sqlC.sql("SELECT city,price,year,manufacturer,Make,odometer,type,paint_color from File")
    val tab2 = tab_1.na.drop(how= "any")
    val tab3 = tab2.withColumn("id",monotonically_increasing_id())
    val temp_v1 = tab3.createOrReplaceTempView("tab3")
    val tab4 = sqlC.sql("SELECT id, city, price, year, CONCAT(manufacturer,' ',make) Cars, odometer, type, paint_color AS Color from tab3")
    tab4.write.format("csv").save("s3://s3-test-550-2019/filtered_cars_data")

  }

}
