class Cars_tab_filter {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SQLContext
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.functions._


    val file = spark.read.format("csv").option("header","true").option('quote', "\"").option("escape", "\"").option("multiLine","true").option("inferSchema","true").load("s3://s3-test-550-2019/craigslistcarsFull.csv")


    val sqlC = new org.apache.spark.sql.SQLContext(sc)
    val temp_v = file.createOrReplaceTempView("File")
    val tab_1 = sqlC.sql("SELECT city,price,year,manufacturer,Make,odometer,type,paint_color from File")
    val temp_v = file.createOrReplaceTempView("File")
    val tab_1 = sqlC.sql("SELECT city,price,year,manufacturer,Make,odometer,type,paint_color from File")
    val tab2 = tab_1.na.drop(how= "any")
    val tab3 = tab2.withColumn("id",monotonicallyIncreasingId)
    val temp_v = tab3.createOrReplaceTempView("tab3")
    val tab4 = sqlC.sql("SELECT id, city, price, year, CONCAT(manufacturer,' ',make) Cars, odometer, type, paint_color AS Color from tab3")
    tab4.write.format("csv").save("s3://s3-test-550-2019/filtered_cars_data")

  }

}
