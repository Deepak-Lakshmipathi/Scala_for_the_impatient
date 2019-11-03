package practice

import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

class Read_from_s3{
  val sess = SparkSession.builder().getOrCreate()
  def dat = sess.read.format("csv").option("header","true").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("inferSchema","true").load("s3://s3-test-550-2019/craigslistcarsFull.csv")
}


class transform {
  val data = new Read_from_s3
  val temp_v = data.dat.createOrReplaceTempView("File")
  val tab_1 = data.sess.sql("SELECT city,price,year,manufacturer,Make,odometer,type,paint_color from File").na.drop(how= "any").withColumn("id",monotonically_increasing_id()).createOrReplaceTempView("tab3")
  def final_tab = data.sess.sql("SELECT id, city, price, year, CONCAT(manufacturer,' ',make) Cars, odometer, type, paint_color AS Color from tab3")
}

class write_to_S3{
  val post_transform= new transform
  val final_tab = post_transform.final_tab
  def write = final_tab.write.mode("overwrite").format("csv").save("s3://s3-test-550-2019/filtered_cars_data")
}

object Car {
  def main(args: Array[String]): Unit = {
    val fin = new write_to_S3
    fin.write

  }

}
